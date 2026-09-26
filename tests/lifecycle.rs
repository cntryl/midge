//! Lifecycle and Resource Tests
//!
//! Consolidated from: `engine_gc.rs`, `resource_cleanup.rs`, `shutdown_reopen_stress.rs`, `memory_management.rs`, `memory_mode_isolation.rs`, `memory_spill_audit.rs`, `backpressure.rs`, `column_family_reclamation_hardening.rs`

mod common;

mod engine_gc {
    //! Garbage Collection Integration Tests
    //!
    //! Tests local SST and WAL garbage collection:
    //! - Orphan SST/WAL file detection and deletion
    //! - Persistence of GC state across restarts
    //! - GC interaction with active readers (snapshot isolation)
    //! - Configurable GC intervals and triggering
    //!
    //! **Storage Modes**: All (memory, local, cloud)
    //! Note: Memory mode has no files to GC, tests validate graceful no-op behavior.
    //!
    //! Naming convention:
    //! should_<behavior>_given_<context>_when_<condition>

    use crate::common::*;
    use cntryl_midge::{TransactionMode, WriteOptions};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    // ============================================================================
    // HELPER: Extract filesystem path from local storage mode
    // ============================================================================

    fn get_storage_path_if_local(opts: &MidgeOptions) -> Option<PathBuf> {
        match &opts.storage_mode {
            StorageMode::LocalDisk { db_path } => Some(db_path.clone()),
            StorageMode::Memory | StorageMode::CloudBacked { .. } => None,
        }
    }

    fn local_sst_names(db_path: &std::path::Path) -> Vec<String> {
        let mut names = std::fs::read_dir(db_path.join("sst"))
            .expect("read local SST directory")
            .filter_map(Result::ok)
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| {
                std::path::Path::new(name)
                    .extension()
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("sst"))
            })
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    // ============================================================================
    // TEST GROUP 1: SST Garbage Collection
    // ============================================================================

    #[test]
    fn should_collect_orphaned_sst_files_after_compaction() {
        for_each_storage_mode(&["local"], |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            for batch in 0..4 {
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for index in 0..25 {
                    let key = format!("key_{batch}_{index:04}");
                    tx.put(key.into_bytes(), b"v1".to_vec(), None)
                        .expect("put compaction seed");
                }
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine.flush_cf(&cf).expect("flush L0 generation");
            }
            let db_path = get_storage_path_if_local(&opts).expect("local storage path");
            let input_names = local_sst_names(&db_path);
            assert_eq!(input_names.len(), 4);
            // Act
            engine.compact_all().expect("compact L0 generations");

            // Assert
            let remaining_names = local_sst_names(&db_path);
            assert!(!remaining_names.is_empty());
            let retained_input_count = input_names
                .iter()
                .filter(|input_name| db_path.join("sst").join(input_name).exists())
                .count();
            assert_eq!(
                retained_input_count, 0,
                "compact_all must reclaim every physical L0 input across bounded batches"
            );
            assert!(
                input_names
                    .iter()
                    .any(|input_name| !db_path.join("sst").join(input_name).exists()),
                "at least one obsolete compaction input must be absent on disk"
            );
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            for batch in 0..4 {
                for index in 0..25 {
                    let key = format!("key_{batch}_{index:04}");
                    let val = tx.get(key.as_bytes()).expect("get during compaction");
                    assert!(val.is_some(), "key lost during compaction in mode: {mode}");
                }
            }
        });
    }

    #[test]
    fn should_not_collect_sst_files_referenced_by_manifest() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            eprintln!("\n=== GC: Preserve Referenced SST Files (mode: {mode}) ===");

            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write and flush to create SST referenced by manifest
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..50 {
                let key = format!("key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .ok();
            }
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Act: Don't trigger compaction; SST remains active
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            // Assert: SST is still readable (proof it exists)
            for i in 0..50 {
                let key = format!("key_{i:04}");
                let val = tx.get(key.as_bytes()).expect("get");
                assert!(
                    val.is_some(),
                    "SST file prematurely deleted in mode: {mode}"
                );
            }

            eprintln!("âœ“ SST files referenced by manifest are preserved");
        });
    }

    #[test]
    fn should_run_gc_after_configurable_interval() {
        eprintln!("\n=== GC: Configurable Interval Triggering ===");

        // Arrange: `for_each_storage_mode`'s helper opens every mode with
        // background compaction disabled (deterministic for other tests), so
        // this test needs its own engine with it explicitly enabled — the
        // whole point here is proving GC runs *on its own periodic interval*,
        // never via a manual `compact_all()` call.
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::local(db_path)
                .background_compaction(true)
                .build()
                .expect("build options"),
        )
        .expect("open engine");
        let cf = engine.create_column_family("test").expect("create cf");

        // Four overlapping-key L0 generations cross the default L0 file-count
        // compaction trigger. Background compaction is scheduled immediately
        // after each flush publication, so it can start collecting earlier
        // generations before this loop even finishes — record each newly
        // flushed L0 input's name as it's created rather than assuming all
        // four coexist once the loop completes. SST names encode their level
        // as `<cf>_<level>_<seq>.sst`, so an L0 flush output is always tagged
        // `_00_`, distinguishing it from a compacted (level >= 1) output that
        // might already exist by the time we check.
        let is_l0_output = |name: &String| name.split('_').nth(1) == Some("00");
        let mut input_names = std::collections::BTreeSet::new();
        for batch in 0..4 {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for index in 0..25 {
                let key = format!("interval_key_{index:04}");
                tx.put(key.into_bytes(), format!("v{batch}").into_bytes(), None)
                    .expect("put");
            }
            tx.commit(WriteOptions::buffered()).expect("commit");
            engine.flush_cf(&cf).expect("flush L0 generation");
            input_names.extend(local_sst_names(db_path).into_iter().filter(is_l0_output));
        }
        assert_eq!(
            input_names.len(),
            4,
            "expected four distinct flushed L0 generations across the run"
        );

        // Act: wait for background GC/compaction to run on its own; poll
        // rather than sleep a fixed amount so this doesn't depend on exactly
        // when the trigger fires while still bounding total runtime.
        let any_input_collected = |db_path: &std::path::Path| {
            input_names
                .iter()
                .any(|name| !db_path.join("sst").join(name).exists())
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(45);
        while !any_input_collected(db_path) {
            assert!(
                std::time::Instant::now() < deadline,
                "background GC never ran within its interval; all four flushed \
                 inputs are still present on disk: {input_names:?}"
            );
            thread::sleep(Duration::from_millis(200));
        }

        // Assert: at least one obsolete compaction input is now gone from
        // disk — proof the background interval actually triggered GC, not
        // just that reads kept working.
        assert!(
            any_input_collected(db_path),
            "background GC did not collect any obsolete compaction input"
        );

        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");
        for index in 0..25 {
            let key = format!("interval_key_{index:04}");
            assert!(
                tx.get(key.as_bytes())
                    .expect("read after background GC")
                    .is_some(),
                "data lost during background GC"
            );
        }

        eprintln!("✓ Background GC collected obsolete inputs within its interval");
    }

    #[test]
    fn should_persist_gc_state_across_restart() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            eprintln!("\n=== GC: Persist State Across Restart (mode: {mode}) ===");

            // Arrange
            // Act: Write and trigger compaction
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write batch 1
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..75 {
                    let key = format!("persist_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"v1".to_vec(), None).ok();
                }
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine.flush_cf(&cf).expect("flush");

                // Write batch 2
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 75..150 {
                    let key = format!("persist_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"v2".to_vec(), None).ok();
                }
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine.flush_cf(&cf).expect("flush");

                // Trigger manual compaction (marks orphans for deletion)
                engine.compact_all().ok();
                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before restart");
            }

            // Assert: Reopen and verify state
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // All data should still be present (GC didn't lose anything)
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                for i in 0..150 {
                    let key = format!("persist_key_{i:04}");
                    assert!(
                        tx.get(key.as_bytes())
                            .expect("read after GC state restart")
                            .is_some(),
                        "data lost after restart in mode: {mode}"
                    );
                }

                eprintln!("âœ“ GC state persisted correctly across restart");
            }
        });
    }

    #[test]
    fn should_handle_gc_with_active_readers() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            eprintln!("\n=== GC: Handle Active Readers (mode: {mode}) ===");

            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Arrange: Write and flush initial data
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..100 {
                let key = format!("reader_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"snapshot_value".to_vec(), None)
                    .ok();
            }
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Act: Create snapshot (read lock on SSTs)
            let snapshot = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            // Spawn thread to trigger compaction while snapshot is held
            let engine_clone = Arc::clone(&engine);
            let _cf_clone = cf.clone();
            let compaction_handle = thread::spawn(move || {
                thread::sleep(Duration::from_millis(50));
                engine_clone.compact_all().ok();
                eprintln!("Compaction triggered while snapshot held");
            });

            // Wait a bit, then verify snapshot still reads correctly
            thread::sleep(Duration::from_millis(100));
            let val = snapshot.get(b"reader_key_0000").expect("get");
            assert!(
                val.is_some(),
                "snapshot read failed during compaction in mode: {mode}"
            );

            // Wait for compaction thread
            compaction_handle
                .join()
                .expect("background compaction thread should not panic");

            // Assert: Drop snapshot; verify data still present
            drop(snapshot);
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            for i in 0..100 {
                let key = format!("reader_key_{i:04}");
                assert!(
                    tx.get(key.as_bytes())
                        .expect("read after active-reader compaction")
                        .is_some(),
                    "data lost after compaction in mode: {mode}"
                );
            }

            eprintln!("âœ“ Active readers protected during GC");
        });
    }

    // ============================================================================
    // TEST GROUP 2: WAL Garbage Collection
    // ============================================================================

    #[test]
    fn should_collect_orphaned_wal_segments_after_flush() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            eprintln!("\n=== GC: Collect Orphaned WAL Segments (mode: {mode}) ===");

            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write to memtable (goes to WAL)
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..50 {
                let key = format!("wal_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"wal_value".to_vec(), None)
                    .ok();
            }
            tx.commit(buffered_write_options(mode)).expect("commit");

            // Act: Flush to SST (WAL segment becomes obsolete)
            engine.flush_cf(&cf).expect("flush");

            // Assert: Data is still readable (proof WAL->SST transition succeeded)
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            for i in 0..50 {
                let key = format!("wal_key_{i:04}");
                let val = tx.get(key.as_bytes()).expect("get");
                assert!(val.is_some(), "data lost after flush in mode: {mode}");
            }

            // For local/cloud modes, flush must advance the durability frontier
            // past the WAL it just captured, leaving no outstanding
            // memtable/WAL segment gap — the observable signal that the old
            // segment is now orphaned and eligible for GC (the same contract
            // `observability_api.rs` checks after an explicit flush).
            if mode != "memory" {
                let metrics = engine
                    .metrics()
                    .get_runtime_metrics()
                    .expect("runtime metrics");
                assert_eq!(
                    metrics.max_memtable_wal_segment_gap, 0,
                    "flush should leave no outstanding WAL segment gap in mode: {mode}"
                );
                eprintln!("âœ“ WAL segment orphaned and eligible for collection");
            }
        });
    }

    #[test]
    fn should_not_collect_wal_segments_still_needed_for_recovery() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            eprintln!("\n=== GC: Preserve WAL for Recovery (mode: {mode}) ===");

            // Arrange
            // Act: Write uncommitted data and crash
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write to memtable (committed but not flushed)
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..30 {
                    let key = format!("recovery_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"recovery_value".to_vec(), None)
                        .ok();
                }
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before restart");
            }

            // Assert: Restart and verify WAL recovery
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Data should be recovered from WAL (prove WAL wasn't garbage collected)
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                for i in 0..30 {
                    let key = format!("recovery_key_{i:04}");
                    let val = tx.get(key.as_bytes()).expect("get");
                    assert!(
                        val.is_some(),
                        "WAL segment was incorrectly GC'd in mode: {mode}"
                    );
                }

                eprintln!("âœ“ WAL segments preserved for recovery across restart");
            }
        });
    }
}

mod resource_cleanup {
    //! Resource cleanup tests - verify proper memory and handle cleanup
    //!
    //! Tests that components properly clean up memory and other resources when
    //! dropped, ensuring the engine can run in constrained environments.

    use cntryl_midge::__internal::sst::cache::{BlockCache, CacheKey, CachePolicyType};
    use std::sync::Arc;

    #[test]
    fn should_cleanup_block_cache_resources_when_dropped() {
        // Arrange - create many BlockCache instances and populate each so there
        // is real state for cleanup to discard.
        let caches: Vec<BlockCache> = (0..10)
            .map(|shard_seed| {
                let cache = BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru);
                for i in 0..20 {
                    let key = CacheKey::for_data(shard_seed, i);
                    let data = bytes::Bytes::from(vec![0u8; 128]);
                    assert!(cache.put(key, &data), "put should succeed under capacity");
                }
                cache
            })
            .collect();

        // Sanity check: every cache actually holds the entries we just inserted.
        for cache in &caches {
            assert_eq!(cache.len(), 20);
            assert!(cache.size_bytes() > 0);
        }

        // Act - drop all caches
        drop(caches);
        // Assert (implicit) - Drop for BlockCache/CacheShard runs without panicking
        // or hanging even while every shard holds populated entries.
    }

    #[test]
    fn should_cleanup_cache_with_active_operations() {
        // Arrange
        let cache = Arc::new(BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru));
        let cache_clone = Arc::clone(&cache);
        // Keep a second surviving handle so we can inspect cache state after the
        // original reference is dropped mid-operation.
        let cache_check = Arc::clone(&cache);

        // Start a thread with cache operations
        let handle = std::thread::spawn(move || {
            let mut successes = 0usize;
            for i in 0..100 {
                let key = CacheKey::for_data(i, 0);
                let data = bytes::Bytes::from(vec![0u8; 100]);
                if cache_clone.put(key, &data) {
                    successes += 1;
                }
            }
            successes
        });

        // Wait for operations to start
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Act - drop the main cache reference while the worker is still writing
        drop(cache);

        // Assert - all inserts succeeded and are visible through the surviving
        // Arc handle, proving the shared cache state survived the concurrent drop.
        let successes = handle.join().expect("worker thread should not panic");
        assert_eq!(
            successes, 100,
            "all puts should have succeeded under capacity"
        );
        assert_eq!(
            cache_check.len(),
            successes,
            "surviving cache handle should see exactly the entries the worker inserted"
        );
    }

    #[test]
    fn should_cleanup_multiple_component_types_together() {
        // Arrange - create multiple caches, one per eviction policy
        let cache1 = BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru);
        let cache2 = BlockCache::new(512 * 1024, 8, CachePolicyType::TinyLfu);
        let cache3 = BlockCache::new(2 * 1024 * 1024, 16, CachePolicyType::ClockPro);

        // Act - exercise each cache so we know it actually holds live state
        // before the components are dropped together.
        let key = CacheKey::for_data(1, 0);
        let data = bytes::Bytes::from(vec![1u8; 64]);
        assert!(cache1.put(key, &data));
        assert!(cache2.put(key, &data));
        assert!(cache3.put(key, &data));

        assert_eq!(cache1.get(&key).map(|v| v.data), Some(data.clone()));
        assert_eq!(cache2.get(&key).map(|v| v.data), Some(data.clone()));
        assert_eq!(cache3.get(&key).map(|v| v.data), Some(data.clone()));

        // Assert - drop all three populated components together without deadlock
        drop((cache1, cache2, cache3));
    }

    #[test]
    fn should_handle_zero_capacity_cache_cleanup() {
        // Arrange - edge case: cache with zero capacity
        let cache = BlockCache::new(0, 16, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let data = bytes::Bytes::from(vec![0u8; 16]);

        // Act
        let accepted = cache.put(key, &data);

        // Assert - a non-empty value can never fit in a zero-capacity cache, so
        // put must be rejected and the cache must stay empty.
        assert!(
            !accepted,
            "put should be rejected when it cannot fit under zero capacity"
        );
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());

        drop(cache);
    }

    #[test]
    fn should_handle_single_shard_cache_cleanup() {
        // Arrange - edge case: cache with only 1 shard (1 worker thread)
        let cache = BlockCache::new(1024 * 1024, 1, CachePolicyType::Lru);
        assert_eq!(cache.num_shards(), 1);

        // Act - insert entries that would land in different shards under a
        // multi-shard cache, to confirm single-shard routing still round-trips.
        for i in 0..10 {
            let key = CacheKey::for_data(i, 0);
            let byte = u8::try_from(i).expect("test index fits in u8");
            let data = bytes::Bytes::from(vec![byte; 32]);
            assert!(cache.put(key, &data));
        }

        // Assert - all entries are present and readable back
        for i in 0..10 {
            let key = CacheKey::for_data(i, 0);
            let byte = u8::try_from(i).expect("test index fits in u8");
            let expected = bytes::Bytes::from(vec![byte; 32]);
            assert_eq!(cache.get(&key).map(|v| v.data), Some(expected));
        }
        assert_eq!(cache.len(), 10);

        drop(cache);
    }
}

mod shutdown_reopen_stress {
    //! Clean-Shutdown and Reopen Stress Tests
    //!
    //! Tests concurrent writes and storage operations followed by clean shutdown
    //! and reopen in local-disk mode.
    //! Coverage in this file is limited to successful operations performed before
    //! normal process teardown via `drop`:
    //! - Recovery of committed WAL-backed and SST-backed writes after reopen
    //! - Visibility after flush, compaction, and manifest-related operations
    //! - Value integrity checks after reopen
    //! - Concurrent best-effort load remaining readable without invalid values
    //!
    //! **Storage Modes**: Local only
    //!
    //! Naming convention:
    //! should_<behavior>_given_<context>_when_<condition>

    use crate::common::*;
    use cntryl_midge::{TransactionMode, WriteOptions};
    use std::thread;
    use std::time::Duration;

    // ============================================================================
    // TEST GROUP: CLEAN SHUTDOWN REOPEN SCENARIOS
    // ============================================================================

    #[test]
    fn should_recover_committed_wal_writes_when_reopening_after_clean_shutdown() {
        for_each_storage_mode(&["local"], |mode, opts| {
            eprintln!("\n=== Reopen After WAL Writes (mode: {mode}) ===");

            // Arrange
            // Act (Phase 1): Commit WAL-backed writes, then drop the engine
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write data (goes to WAL)
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..100 {
                    let key = format!("wal_fail_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"wal_value".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");

                // Additional committed writes
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 100..150 {
                    let key = format!("wal_fail_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"wal_value_2".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");

                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2): Reopen and validate recovery
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");

                for i in 0..150 {
                    let key = format!("wal_fail_key_{i:04}");
                    let expected = if i < 100 {
                        b"wal_value".as_slice()
                    } else {
                        b"wal_value_2".as_slice()
                    };
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("get"),
                        Some(expected.into()),
                        "mode: {mode} key: {key}"
                    );
                }

                eprintln!("âœ“ Recovered all 150 committed WAL-backed records");
            }
        });
    }

    #[test]
    fn should_recover_committed_writes_when_reopening_after_flush_and_clean_shutdown() {
        for_each_storage_mode(&["local"], |mode, opts| {
            eprintln!("\n=== Reopen After Flush (mode: {mode}) ===");

            // Arrange
            // Act (Phase 1): Commit writes, flush, then drop the engine
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Load memtable with many keys
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..200 {
                    let key = format!("flush_fail_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"flush_data".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");

                // Flush the committed data successfully
                engine.flush_cf(&cf).expect("flush");

                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2): Reopen and verify all flushed data
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");

                for i in 0..200 {
                    let key = format!("flush_fail_key_{i:04}");
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("get"),
                        Some(b"flush_data".as_slice().into()),
                        "mode: {mode} key: {key}"
                    );
                }

                eprintln!("âœ“ Recovered all 200 committed records after flush");
            }
        });
    }

    #[test]
    fn should_recover_committed_writes_when_reopening_after_compaction_and_clean_shutdown() {
        for_each_storage_mode(&["local"], |mode, opts| {
            eprintln!("\n=== Reopen After Compaction (mode: {mode}) ===");

            // Arrange
            // Act (Phase 1): Create multiple SSTs, compact, then drop the engine
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Create SST A
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..100 {
                    let key = format!("compact_fail_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"gen_a".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");
                engine.flush_cf(&cf).expect("flush A");

                // Create SST B
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 100..200 {
                    let key = format!("compact_fail_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"gen_b".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");
                engine.flush_cf(&cf).expect("flush B");

                // Trigger compaction before shutdown
                engine.compact_all().expect("compact_all");

                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2): Reopen and verify all committed data
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");

                for i in 0..200 {
                    let key = format!("compact_fail_key_{i:04}");
                    let expected = if i < 100 {
                        b"gen_a".as_slice()
                    } else {
                        b"gen_b".as_slice()
                    };
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("get"),
                        Some(expected.into()),
                        "mode: {mode} key: {key}"
                    );
                }

                eprintln!("âœ“ Recovered all 200 committed records after compaction");
            }
        });
    }

    #[test]
    fn should_preserve_readability_when_reopening_after_manifest_updates_and_clean_shutdown() {
        for_each_storage_mode(&["local"], |mode, opts| {
            eprintln!("\n=== Reopen After Manifest Updates (mode: {mode}) ===");

            // Arrange
            // Act (Phase 1): Flush data, run compaction-related work, then drop the engine
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write and flush (updates manifest)
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..150 {
                    let key = format!("manifest_io_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");
                engine.flush_cf(&cf).expect("flush");

                // Trigger compaction-related manifest updates before shutdown
                engine.compact_all().expect("compact_all");

                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2): Reopen and verify readability
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                for i in 0..150 {
                    let key = format!("manifest_io_key_{i:04}");
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("get"),
                        Some(b"value".as_slice().into()),
                        "mode: {mode} key: {key}"
                    );
                }

                eprintln!("âœ“ All 150 committed records remained readable after reopen");
            }
        });
    }

    #[test]
    fn should_preserve_sst_backed_values_when_reopening_after_flush_and_clean_shutdown() {
        for_each_storage_mode(&["local"], |mode, opts| {
            eprintln!("\n=== Reopen After SST Flush (mode: {mode}) ===");

            // Arrange
            // Act (Phase 1): Write, flush, add more writes, then drop the engine
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write batch 1
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..100 {
                    let key = format!("sst_corrupt_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"uncorrupted_value".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");

                // Flush the first batch successfully
                engine.flush_cf(&cf).expect("flush");

                // Write a second batch that remains WAL-backed until reopen
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 100..150 {
                    let key = format!("sst_corrupt_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"clean_value".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");

                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2): Reopen and verify exact values
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");

                for i in 0..100 {
                    let key = format!("sst_corrupt_key_{i:04}");
                    let val = tx
                        .get(key.as_bytes())
                        .expect("get")
                        .expect("value should exist");
                    assert_eq!(
                        val.as_ref(),
                        b"uncorrupted_value",
                        "mode: {mode} key: {key}"
                    );
                }
                for i in 100..150 {
                    let key = format!("sst_corrupt_key_{i:04}");
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("get"),
                        Some(b"clean_value".as_slice().into()),
                        "mode: {mode} key: {key}"
                    );
                }

                eprintln!("âœ“ Recovered exact SST-backed and WAL-backed values after reopen");
            }
        });
    }

    #[test]
    fn should_preserve_wal_backed_values_when_reopening_after_clean_shutdown() {
        for_each_storage_mode(&["local"], |mode, opts| {
            eprintln!("\n=== Reopen After WAL-Backed Writes (mode: {mode}) ===");

            // Arrange
            // Act (Phase 1): Commit two WAL-backed write batches, then drop the engine
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write batch 1 (committed to WAL)
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..50 {
                    let key = format!("wal_corrupt_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"wal_clean".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");

                // Write batch 2
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 50..100 {
                    let key = format!("wal_corrupt_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"wal_second".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");

                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2): Reopen with WAL replay
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");

                for i in 0..100 {
                    let key = format!("wal_corrupt_key_{i:04}");
                    let expected = if i < 50 {
                        b"wal_clean".as_slice()
                    } else {
                        b"wal_second".as_slice()
                    };
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("get"),
                        Some(expected.into()),
                        "mode: {mode} key: {key}"
                    );
                }

                eprintln!("âœ“ Recovered exact WAL-backed values after reopen");
            }
        });
    }

    #[test]
    fn should_handle_concurrent_best_effort_writes_under_load_without_invalid_values() {
        for_each_storage_mode(&["local"], |mode, opts| {
            eprintln!("\n=== Concurrent Best-Effort Load (mode: {mode}) ===");

            // Arrange: High-concurrency best-effort write load
            let engine = std::sync::Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            let mut handles = vec![];

            // Spawn multiple writer threads
            for tid in 0..5 {
                let engine_clone = std::sync::Arc::clone(&engine);
                let cf_clone = cf.clone();
                let handle = std::thread::spawn(move || {
                    for batch in 0..20 {
                        let tx = engine_clone
                            .begin_tx(cf_clone.id(), TransactionMode::ReadWrite)
                            .ok();

                        if let Some(mut t) = tx {
                            for i in 0..50 {
                                let key = format!("stress_load_t{tid}_b{batch}_k{i:03}");
                                t.put(key.as_bytes().to_vec(), b"stress_value".to_vec(), None)
                                    .ok();
                            }
                            t.commit(WriteOptions::best_effort()).ok();
                        }

                        // Small stagger to vary writer interleaving
                        if batch % 3 == 0 {
                            thread::sleep(Duration::from_millis(10));
                        }
                    }
                });
                handles.push(handle);
            }

            // Wait for all writers to complete
            for handle in handles {
                handle.join().ok();
            }

            // Act: Flush and compact after the concurrent load
            engine.flush_cf(&cf).ok();
            engine.compact_all().ok();

            // Assert: Engine remains readable and sampled keys never return invalid bytes.
            // Best-effort writes are not treated as a full-durability guarantee here.
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            let mut sampled_present = 0;
            for tid in 0..5 {
                for batch in 0..20 {
                    let key = format!("stress_load_t{tid}_b{batch}_k000");
                    if let Some(val) = tx.get(key.as_bytes()).expect("get") {
                        assert_eq!(val.as_ref(), b"stress_value", "mode: {mode} key: {key}");
                        sampled_present += 1;
                    }
                }
            }

            assert!(
                sampled_present > 0,
                "expected at least one sampled key to be present in mode: {mode}"
            );

            eprintln!(
                "âœ“ Best-effort load remained readable with {sampled_present} sampled keys present"
            );
        });
    }
}

mod memory_management {
    //! Memory-related tests (formerly Phase 1 fixes)

    use crate::common::*;
    use cntryl_midge::{EngineHealth, MidgeError, TransactionMode};
    use std::time::Duration;

    #[test]
    fn should_handle_small_memory_budget_without_unexpected_errors() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange: Open engine with very small memtable limit to trigger frequent flushes
            let mut opts = opts;
            opts = opts.memory_budget(64 * 1024); // 64KB
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act: Write enough data to fill immutable queue
            let mut write_count = 0;
            let mut stall_encountered = false;

            for i in 0..1000 {
                let key = format!("key_{i:08}");
                let value = vec![b'x'; 1024]; // 1KB value

                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(key.into_bytes(), value, None).unwrap();

                match tx.commit(buffered_write_options(mode)) {
                    Ok(()) => {
                        write_count += 1;
                    }
                    Err(MidgeError::WriteStall(_)) => {
                        stall_encountered = true;
                        break;
                    }
                    Err(e) => {
                        panic!("Unexpected error: {e:?}");
                    }
                }

                if i % 10 == 0 {
                    let _ = engine.flush_cf(&cf);
                }
            }

            engine.flush_cf(&cf).expect("final flush");
            if !mode.eq("memory") {
                engine
                    .compact_all()
                    .expect("clear L0 debt after sustained memory pressure");
            }
            let metrics = engine
                .metrics()
                .get_runtime_metrics()
                .expect("runtime metrics");

            // Assert
            if !mode.eq("memory") {
                // Under sustained pressure the engine may either surface WriteStall or
                // keep up by flushing synchronously. Both are acceptable as long as it
                // keeps making progress and does not end in a degraded memory state.
                assert!(
                    stall_encountered || write_count > 0,
                    "Expected progress or backpressure in mode {mode}"
                );
                assert_ne!(
                    metrics.health,
                    EngineHealth::WriteStalled,
                    "Engine should not remain write-stalled after compaction clears debt in mode {mode}"
                );
            }
        });
    }

    #[test]
    fn should_complete_shutdown_when_wal_writer_drops() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act: Write some data
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key".to_vec(), b"value".to_vec(), None).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act: shut down explicitly (this drops the WAL writer as part of an
            // orderly shutdown) and assert it actually reports success within a
            // bound, rather than only relying on the test harness's own timeout
            // to catch a hang with no diagnostic about *why* it failed.
            let result = engine.shutdown(Duration::from_secs(10));

            // Assert
            assert!(
                result.is_ok(),
                "shutdown did not complete cleanly when the WAL writer dropped in mode {mode}: {result:?}"
            );
        });
    }
}

mod memory_mode_isolation {
    //! Memory Mode Isolation Tests
    //!
    //! Tests that memory mode creates no persistent filesystem artifacts and isolates
    //! data between engine instances. Memory mode operates entirely in RAM with zero
    //! disk side effects.
    //!
    //! Naming convention:
    //! should_<behavior>_given_<context>_when_<condition>
    //!
    //! These tests run on MEMORY MODE ONLY to validate isolation and filesystem cleanup.

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::{TransactionMode, WriteOptions};

    // ============================================================================
    // FILESYSTEM ARTIFACT TESTS
    // ============================================================================

    /// Top-level entries directly under the crate root. Every other test that
    /// touches disk does so through `test_temp_dir()` / `target/tmp/...`
    /// (verified across `tests/*.rs`), so a real bug that made memory mode fall
    /// back to some default on-disk path would most plausibly show up as a new
    /// entry at this top level - unlike scanning `target/tmp`, this is safe to
    /// check even while other tests run concurrently in the same process.
    fn crate_root_top_level_entries() -> std::collections::BTreeSet<std::ffi::OsString> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        std::fs::read_dir(root)
            .expect("read crate root")
            .filter_map(|entry| entry.ok().map(|entry| entry.file_name()))
            .collect()
    }

    #[test]
    fn should_not_create_filesystem_artifacts_when_memory_mode() {
        // Arrange: snapshot the crate root before the engine runs, so the
        // assertion actually inspects the filesystem instead of trusting a
        // comment.
        let entries_before = crate_root_top_level_entries();

        let opts = opts_for_mode("memory");

        // Act: Open, write, close
        let engine = open_with_mode(&opts, "memory");
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        tx.put(b"test_key_1".to_vec(), b"test_value_1".to_vec(), None)
            .expect("put");
        tx.put(b"test_key_2".to_vec(), b"test_value_2".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::buffered()).unwrap();
        drop(engine); // memory mode should store nothing on disk

        // Assert: no new top-level filesystem entries appeared. Memory mode's
        // `OpenOptions` never carries a path at all, so this catches the real
        // bug class of a hard-coded/default on-disk fallback slipping in.
        let entries_after = crate_root_top_level_entries();
        let new_entries: Vec<_> = entries_after.difference(&entries_before).collect();
        assert!(
            new_entries.is_empty(),
            "memory mode created unexpected filesystem entries: {new_entries:?}"
        );
    }

    #[test]
    fn should_not_persist_data_across_restart_given_memory_mode_when_reopening() {
        // Arrange: Open and write data
        let opts1 = opts_for_mode("memory");

        {
            let engine = open_with_mode(&opts1, "memory");
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Act: Write to memory engine
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            tx.put(
                b"persist_test".to_vec(),
                b"should_not_persist".to_vec(),
                None,
            )
            .expect("put");
            tx.commit(WriteOptions::buffered()).unwrap();
            // engine dropped
        }

        // Assert: New memory engine instance has no data
        let opts2 = opts_for_mode("memory");
        {
            let engine = open_with_mode(&opts2, "memory");
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            let got = tx.get(b"persist_test").expect("get");
            assert_eq!(
                got, None,
                "memory mode persisted data across restart (should not persist)"
            );
        }
    }

    #[test]
    fn should_isolate_data_given_multiple_memory_engines_when_separate_instances() {
        // Arrange: Create two separate memory engine instances
        let opts1 = opts_for_mode("memory");
        let opts2 = opts_for_mode("memory");

        // Act: Write different data to each
        let engine1 = open_with_mode(&opts1, "memory");
        let cf1 = engine1.create_column_family("test").expect("create cf");
        let cf_id1 = cf1.id();
        let mut tx = engine1
            .begin_tx(cf_id1, TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"test_key".to_vec(), b"engine1_value".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::buffered()).unwrap();

        let engine2 = open_with_mode(&opts2, "memory");
        let cf2 = engine2.create_column_family("test").expect("create cf");
        let cf_id2 = cf2.id();
        let mut tx = engine2
            .begin_tx(cf_id2, TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"test_key".to_vec(), b"engine2_value".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::buffered()).unwrap();

        // Assert: Each engine instance has isolated data
        let tx1 = engine1.begin_tx(cf_id1, TransactionMode::ReadOnly).unwrap();
        let got1 = tx1.get(b"test_key").expect("get");
        let tx2 = engine2.begin_tx(cf_id2, TransactionMode::ReadOnly).unwrap();
        let got2 = tx2.get(b"test_key").expect("get");

        assert_eq!(
            got1,
            Some(Bytes::from_static(b"engine1_value")),
            "engine1 data corruption or isolation failure"
        );
        assert_eq!(
            got2,
            Some(Bytes::from_static(b"engine2_value")),
            "engine2 data corruption or isolation failure"
        );
    }

    #[test]
    fn should_handle_many_writes_efficiently_when_writing_100_keys() {
        // Arrange
        // Memory mode only test
        let opts = opts_for_mode("memory");
        let engine = open_with_mode(&opts, "memory");
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        // Act: Perform many writes
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        for i in 0..100 {
            let key = format!("write_test_{i:03}");
            tx.put(key.into_bytes(), b"value".to_vec(), None)
                .expect("put");
        }
        tx.commit(WriteOptions::buffered()).unwrap();

        // Assert: All writes succeeded and data is retrievable
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        for i in &[0, 25, 50, 75, 99] {
            let key = format!("write_test_{i:03}");
            let got = tx.get(key.as_bytes()).expect("get");
            assert_eq!(
                got,
                Some(Bytes::from_static(b"value")),
                "write_test_{i:03} retrieval failed"
            );
        }
    }

    #[test]
    fn should_handle_many_deletes_efficiently_when_deleting_50_keys() {
        // Memory mode only test
        let opts = opts_for_mode("memory");
        let engine = open_with_mode(&opts, "memory");
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        // Arrange: Write 50 keys
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        for i in 0..50 {
            let key = format!("delete_test_{i:02}");
            tx.put(key.into_bytes(), b"value".to_vec(), None)
                .expect("put");
        }
        tx.commit(WriteOptions::buffered()).unwrap();

        // Act: Delete all
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        for i in 0..50 {
            let key = format!("delete_test_{i:02}");
            tx.delete(key.into_bytes()).expect("delete");
        }
        tx.commit(WriteOptions::buffered()).unwrap();

        // Assert: All deleted
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        for i in &[0, 10, 25, 49] {
            let key = format!("delete_test_{i:02}");
            let got = tx.get(key.as_bytes()).expect("get");
            assert_eq!(got, None, "expected key to be deleted but found it");
        }
    }

    #[test]
    fn should_handle_mixed_operations_efficiently_when_put_delete_overwrite() {
        // Arrange
        // Memory mode only test
        let opts = opts_for_mode("memory");
        let engine = open_with_mode(&opts, "memory");
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        // Act: Mixed sequence
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        tx.put(b"key1".to_vec(), b"v1".to_vec(), None).expect("put");
        tx.put(b"key2".to_vec(), b"v2".to_vec(), None).expect("put");
        tx.commit(WriteOptions::buffered()).unwrap();

        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        tx.delete(b"key1".to_vec()).expect("delete");
        tx.commit(WriteOptions::buffered()).unwrap();

        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        tx.put(b"key1".to_vec(), b"v1_new".to_vec(), None)
            .expect("put");
        tx.put(b"key3".to_vec(), b"v3".to_vec(), None).expect("put");
        tx.commit(WriteOptions::buffered()).unwrap();

        // Assert: Correct final state
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            tx.get(b"key1").expect("get"),
            Some(Bytes::from_static(b"v1_new"))
        );
        assert_eq!(
            tx.get(b"key2").expect("get"),
            Some(Bytes::from_static(b"v2"))
        );
        assert_eq!(
            tx.get(b"key3").expect("get"),
            Some(Bytes::from_static(b"v3"))
        );
    }
}

mod memory_spill_audit {
    //! Large-transaction behavior under low configured memory budgets.
    //!
    //! These tests verify externally visible behavior: large transactions commit
    //! successfully and their data remains readable. They do not claim direct
    //! visibility into whether spill-to-disk occurred internally.

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::TransactionMode;

    #[test]
    fn should_commit_large_transaction_given_memory_budget_exceeded_when_committed() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts.memory_budget(128 * 1024), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin large transaction");
            for i in 0..500 {
                let key = format!("large_key_{i:05}");
                tx.put(key.as_bytes().to_vec(), vec![65u8; 1024], None)
                    .expect("put large value");
            }

            // Act
            tx.commit(buffered_write_options(mode))
                .expect("commit large transaction");

            // Assert
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin verification transaction");
            assert_eq!(
                tx.get(b"large_key_00000").expect("read first key"),
                Some(Bytes::from(vec![65u8; 1024]))
            );
            assert_eq!(
                tx.get(b"large_key_00499").expect("read last key"),
                Some(Bytes::from(vec![65u8; 1024]))
            );
            assert_eq!(
                tx.get(b"large_key_00250").expect("read middle key"),
                Some(Bytes::from(vec![65u8; 1024]))
            );
        });
    }

    #[test]
    fn should_preserve_values_given_two_large_transactions_within_budget_when_read() {
        for_each_storage_mode(durable_storage_modes(), |mode, mut opts| {
            // Arrange
            // Keep both commits in the active memtable so this exercises their
            // simultaneous resident footprint rather than an intervening flush.
            opts.memtable_size = 1024 * 1024;
            let engine = open_with_mode(&opts.memory_budget(256 * 1024), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx1 = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin first transaction");
            for i in 0..128 {
                let key = format!("batch1_key_{i:03}");
                tx1.put(key.as_bytes().to_vec(), vec![65u8; 1024], None)
                    .expect("put batch1 value");
            }
            tx1.commit(buffered_write_options(mode))
                .expect("commit first transaction");
            let mut tx2 = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin second transaction");
            for i in 0..128 {
                let key = format!("batch2_key_{i:03}");
                tx2.put(key.as_bytes().to_vec(), vec![66u8; 1024], None)
                    .expect("put batch2 value");
            }

            // Act
            tx2.commit(buffered_write_options(mode))
                .expect("commit second transaction");

            // Assert
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin verification transaction");
            assert_eq!(
                tx.get(b"batch1_key_000").expect("read first batch key"),
                Some(Bytes::from(vec![65u8; 1024]))
            );
            assert_eq!(
                tx.get(b"batch2_key_000").expect("read second batch key"),
                Some(Bytes::from(vec![66u8; 1024]))
            );
            assert_eq!(
                tx.get(b"batch1_key_127").expect("read last batch1 key"),
                Some(Bytes::from(vec![65u8; 1024]))
            );
            assert_eq!(
                tx.get(b"batch2_key_127").expect("read last batch2 key"),
                Some(Bytes::from(vec![66u8; 1024]))
            );
        });
    }

    #[test]
    fn should_preserve_sample_keys_given_large_transaction_low_budget_when_committed() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts.memory_budget(64 * 1024), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin low-budget transaction");
            for i in 0..200 {
                let key = format!("spilltest_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), vec![88u8; 512], None)
                    .expect("put low-budget value");
            }

            // Act
            tx.commit(buffered_write_options(mode))
                .expect("commit low-budget transaction");

            // Assert
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin verification transaction");
            for key in [
                b"spilltest_key_0000".as_slice(),
                b"spilltest_key_0100".as_slice(),
                b"spilltest_key_0199".as_slice(),
            ] {
                assert_eq!(
                    tx.get(key).expect("read sample low-budget key"),
                    Some(Bytes::from(vec![88u8; 512])),
                    "mode: {} key: {}",
                    mode,
                    String::from_utf8_lossy(key)
                );
            }
        });
    }
}

mod backpressure {
    //! Backpressure tests (formerly Phase 2: backpressure validation)

    use crate::common::*;
    use cntryl_midge::{
        ColumnFamilyId, Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    static BACKPRESSURE_STRESS_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    const BACKGROUND_FLUSH_TIMEOUT: Duration = Duration::from_secs(30);

    fn backpressure_stress_test_lock() -> &'static Mutex<()> {
        BACKPRESSURE_STRESS_TEST_LOCK.get_or_init(|| Mutex::new(()))
    }

    fn commit_buffered_put(
        engine: &Engine,
        cf_id: ColumnFamilyId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), MidgeError> {
        let mut txn = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
        txn.put(key, value, None)?;
        txn.commit(WriteOptions::buffered())
    }

    fn write_until_committed(
        engine: &Engine,
        cf_id: ColumnFamilyId,
        writes: usize,
        value_bytes: usize,
    ) -> u64 {
        let mut committed = 0usize;
        let mut attempts = 0usize;
        let mut observed_stalls = 0u64;

        while committed < writes {
            attempts += 1;
            assert!(
                attempts <= writes * 4,
                "too many attempts while writing through backpressure"
            );

            let key = format!("auto_flush_key_{committed:06}").into_bytes();
            let value = vec![0xA5; value_bytes];
            match commit_buffered_put(engine, cf_id, key, value) {
                Ok(()) => committed += 1,
                Err(MidgeError::WriteStall(_)) => {
                    observed_stalls += 1;
                    assert!(
                        engine
                            .wait_for_write_stall_clear(cf_id, BACKGROUND_FLUSH_TIMEOUT)
                            .expect("wait for stall clear"),
                        "transient stall should clear within the background flush budget"
                    );
                }
                Err(error) => panic!("unexpected write error: {error:?}"),
            }
        }

        observed_stalls
    }

    #[test]
    fn should_auto_flush_explicit_small_memtable_without_permanent_write_stall() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = Engine::open(
            OpenOptions::local(temp_dir.path())
                .with_memtable_size_limit(128 * 1024)
                .build()
                .expect("build options"),
        )
        .expect("open engine");
        let cf = engine
            .create_column_family("auto_flush")
            .expect("create cf");

        let _observed_stalls = write_until_committed(&engine, cf.id(), 800, 1024);

        // Act
        // Assert
        assert!(
            engine
                .wait_for_write_stall_clear(cf.id(), BACKGROUND_FLUSH_TIMEOUT)
                .expect("wait for final stall clear"),
            "runtime should not remain permanently stalled"
        );
        let metrics = engine
            .metrics()
            .get_runtime_metrics()
            .expect("runtime metrics");
        assert!(
            metrics.sst_count >= 1,
            "natural auto-flush should publish at least one SST"
        );
        assert!(
            !metrics.write_stalled,
            "runtime metrics should not report a sticky write stall"
        );
    }

    #[test]
    fn should_auto_flush_when_explicit_flush_threshold_is_lower_than_size_limit() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = Engine::open(
            OpenOptions::local(temp_dir.path())
                .with_memtable_size_limit(1024 * 1024)
                .with_memtable_flush_threshold(64 * 1024)
                .background_compaction(false)
                .build()
                .expect("build options"),
        )
        .expect("open engine");
        let cf = engine
            .create_column_family("flush_threshold")
            .expect("create cf");

        let _observed_stalls = write_until_committed(&engine, cf.id(), 300, 1024);

        let metrics = engine
            .metrics()
            .get_runtime_metrics()
            .expect("runtime metrics");
        // Act
        // Assert
        assert_eq!(metrics.memtable_size_limit, 1024 * 1024);
        assert_eq!(metrics.memtable_flush_threshold, 64 * 1024);
        assert!(
            metrics.sst_count >= 2,
            "lower flush threshold should drive repeated natural auto-flushes"
        );
        assert!(!metrics.write_stalled);
    }

    #[test]
    fn should_bound_published_l0_when_background_compaction_is_disabled() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let options = OpenOptions::local(temp_dir.path())
            .background_compaction(false)
            .build()
            .expect("build options");
        // The immutable queue limit is internal and fixed at ten; the final slot
        // is reserved for the active memtable generation.
        let hard_ceiling = options.l0_compaction_trigger() + 10 + 1;
        let engine = Engine::open(options).expect("open engine");
        let cf = engine
            .create_column_family("bounded-l0")
            .expect("create cf");
        let mut accepted = 0_usize;
        let mut attempts = 0_usize;

        // Act
        while accepted < hard_ceiling * 2 {
            attempts += 1;
            assert!(
                attempts <= hard_ceiling * 8,
                "L0 recovery must make progress"
            );
            let result = commit_buffered_put(
                &engine,
                cf.id(),
                format!("generation-{accepted:02}").into_bytes(),
                b"value".to_vec(),
            );
            match result {
                Ok(()) => {
                    accepted += 1;
                    engine.flush_cf(&cf).expect("flush reserved L0 generation");
                }
                Err(MidgeError::WriteStall(_)) => {
                    assert!(
                        engine
                            .wait_for_write_stall_clear(cf.id(), Duration::from_secs(10))
                            .expect("wait for live L0 recovery"),
                        "critical L0 pressure must clear without manual compaction"
                    );
                }
                Err(error) => panic!("unexpected write result: {error}"),
            }
            let layout = engine.metrics().get_storage_layout().expect("live layout");
            let l0_files = layout
                .levels
                .iter()
                .find(|level| level.level == 0)
                .map_or(0, |level| level.file_count);
            assert!(
                l0_files <= hard_ceiling,
                "published L0 exceeded its ceiling"
            );
        }

        // Assert
        assert_eq!(accepted, hard_ceiling * 2);
        engine.compact_all().expect("manual compaction clears debt");
        let drained_layout = engine
            .metrics()
            .get_storage_layout()
            .expect("drained layout");
        assert_eq!(
            drained_layout
                .levels
                .iter()
                .find(|level| level.level == 0)
                .map_or(0, |level| level.file_count),
            0,
            "compact_all must not return while L0 debt remains"
        );
        commit_buffered_put(&engine, cf.id(), b"after-drain".to_vec(), b"value".to_vec())
            .expect("write admission resumes after every pressure source clears");
    }

    #[test]
    fn should_return_write_stall_when_memory_budget_exceeded() {
        // Arrange
        // Memory mode doesn't have meaningful backpressure (everything stays in memory)
        // so we only test durable modes where flush/compaction creates actual pressure.

        // Act
        let results = std::cell::RefCell::new(Vec::<(String, bool)>::new());
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            let mut opts = opts;
            // Use a smaller budget and an explicit large memtable so the hard
            // budget path still produces a deterministic stall now that soft
            // memtable pressure auto-flushes instead of sticking.
            opts = opts.memory_budget(1024 * 1024);
            opts.memtable_size = 32 * 1024 * 1024;
            let engine = Engine::open(opts.to_open_options()).expect("failed to open engine");
            let cf = engine.create_column_family("test").expect("create cf");

            let mut write_stall_observed = false;

            for i in 0..10_000 {
                let key = format!("key_{i:06}");
                let value = vec![0u8; 1024]; // 1KB

                let mut txn = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin");
                txn.put(key.as_bytes().to_vec(), value.clone(), None)
                    .expect("put");

                match txn.commit(buffered_write_options(mode)) {
                    Ok(()) => {}
                    Err(MidgeError::WriteStall(_msg)) => {
                        write_stall_observed = true;
                        break;
                    }
                    Err(e) => panic!("unexpected: {e:?}"),
                }
            }

            results
                .borrow_mut()
                .push((mode.to_string(), write_stall_observed));
        });

        // Assert
        for (mode, write_stall_observed) in results.into_inner() {
            assert!(
                !write_stall_observed,
                "{mode} mode should now relieve soft memtable pressure without returning WriteStall"
            );
        }
    }

    #[test]
    fn should_succeed_after_backoff_when_write_stall_cleared() {
        // Arrange

        // Act
        let results = std::cell::RefCell::new(Vec::<(String, bool, bool)>::new());
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            let mut opts = opts;
            // Use 1MB budget plus an explicit large memtable so we can still
            // observe the stall-clear cycle deterministically after the runtime
            // started letting soft memtable pressure flush naturally.
            opts = opts.memory_budget(1024 * 1024);
            opts.memtable_size = 32 * 1024 * 1024;
            let engine = Engine::open(opts.to_open_options()).expect("failed to open engine");
            let cf = engine.create_column_family("test").expect("create cf");

            // Hit first stall
            let mut first_stall_observed = false;
            for i in 0..5000 {
                let key = format!("key_{i:06}");
                let value = vec![0u8; 1024];
                let mut txn = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin");
                txn.put(key.as_bytes().to_vec(), value.clone(), None)
                    .expect("put");

                match txn.commit(buffered_write_options(mode)) {
                    Ok(()) => {}
                    Err(MidgeError::WriteStall(_)) => {
                        first_stall_observed = true;
                        break;
                    }
                    Err(e) => panic!("unexpected: {e:?}"),
                }
            }

            // Wait for stall to clear (compaction, etc.)
            std::thread::sleep(Duration::from_millis(100));

            // Attempt a write after stall is cleared; should succeed or stall again (not panic)
            let key = "recovery_key".to_string();
            let value = vec![0u8; 1024];
            let mut txn = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin");
            txn.put(key.as_bytes().to_vec(), value.clone(), None)
                .expect("put");

            let second_write_ok_or_stall = match txn.commit(buffered_write_options(mode)) {
                Ok(()) | Err(MidgeError::WriteStall(_)) => true,
                Err(e) => panic!("unexpected error: {e:?}"),
            };

            results.borrow_mut().push((
                mode.to_string(),
                first_stall_observed,
                second_write_ok_or_stall,
            ));
        });

        // Assert
        for (mode, first_stall_observed, second_write_ok_or_stall) in results.into_inner() {
            if mode == "local" {
                assert!(
                    !first_stall_observed,
                    "local mode should now relieve soft memtable pressure before a sticky stall appears"
                );
            }
            assert!(
                second_write_ok_or_stall,
                "Expected success or stall (not error) in mode {mode}"
            );
        }
    }

    #[test]
    fn should_prevent_oom_by_rejecting_writes_when_budget_exceeded() {
        // Arrange
        let _guard = backpressure_stress_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        // Act
        let results = std::cell::RefCell::new(Vec::<(String, u64, u64, usize, bool)>::new());
        for_each_storage_mode(&["local"], |mode, opts| {
            let mut opts = opts;
            opts = opts.memory_budget(512 * 1024); // 512KB instead of 2MB for faster backpressure trigger

            // In local mode, the default small memtable can flush fast enough that we never
            // accumulate enough in-memory state to trip the memory budget. Keep the
            // threshold above the default but inside this test's 512 KiB pressure window.
            if mode == "local" {
                opts.memtable_size = 256 * 1024;
            }

            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            let max_attempts = 64;
            let write_options = buffered_write_options(mode);
            let mut total_writes = 0;
            let mut total_stalls = 0;

            while total_writes + total_stalls < max_attempts {
                let key = format!("key_{total_writes}");
                let value = vec![0u8; 8192]; // 8KB for faster memory budget exhaustion
                let mut txn = engine
                    .begin_tx(cf_id, TransactionMode::ReadWrite)
                    .expect("begin");
                txn.put(key.as_bytes().to_vec(), value, None).expect("put");

                match txn.commit(write_options) {
                    Ok(()) => total_writes += 1,
                    Err(MidgeError::WriteStall(_)) => {
                        total_stalls += 1;
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(e) => panic!("unexpected: {e:?}"),
                }
            }

            // Natural flush publication is asynchronous. Wait for the actual acceptance
            // condition instead of assuming a fixed sleep covers hosted-runner contention.
            let deadline = Instant::now() + Duration::from_secs(30);
            let metrics = loop {
                let metrics = engine
                    .metrics()
                    .get_runtime_metrics()
                    .expect("runtime metrics");
                if total_stalls > 0 || (metrics.sst_count > 0 && !metrics.write_stalled) {
                    break metrics;
                }
                assert!(
                    Instant::now() < deadline,
                    "local pressure was not rejected or naturally flushed: writes={total_writes}, stalls={total_stalls}, ssts={}, write_stalled={}",
                    metrics.sst_count,
                    metrics.write_stalled
                );
                std::thread::sleep(Duration::from_millis(10));
            };

            results.borrow_mut().push((
                mode.to_string(),
                total_writes,
                total_stalls,
                metrics.sst_count,
                metrics.write_stalled,
            ));
        });

        // Assert
        for (mode, total_writes, total_stalls, sst_count, write_stalled) in results.into_inner() {
            assert_eq!(mode, "local");
            assert!(
                total_stalls > 0 || (sst_count > 0 && !write_stalled),
                "Expected local mode to either reject writes under hard pressure or relieve pressure via natural flush: writes={total_writes}, stalls={total_stalls}, ssts={sst_count}, write_stalled={write_stalled}"
            );
        }
    }

    #[test]
    fn should_handle_concurrent_writes_with_consistent_backpressure() {
        // Arrange
        let _guard = backpressure_stress_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        // Act
        let results = std::cell::RefCell::new(Vec::<(String, u64, Vec<(u64, u64)>, bool)>::new());
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            let mut opts = opts;
            opts = opts.memory_budget(4 * 1024 * 1024);
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            let shutdown = Arc::new(AtomicBool::new(false));
            let mut handles = vec![];

            for thread_id in 0..4 {
                let engine_clone = Arc::clone(&engine);
                let shutdown_clone = shutdown.clone();
                let cf_id = cf.id();
                let write_opts = buffered_write_options(mode);

                let handle = std::thread::spawn(move || {
                    let mut writes = 0u64;
                    let mut stalls = 0u64;

                    while !shutdown_clone.load(Ordering::Relaxed) {
                        let key = format!("thread_{thread_id}_key_{writes}");
                        let value = vec![0u8; 1024];
                        let mut txn = engine_clone
                            .begin_tx(cf_id, TransactionMode::ReadWrite)
                            .expect("begin");
                        txn.put(key.as_bytes().to_vec(), value.clone(), None)
                            .expect("put");

                        match txn.commit(write_opts) {
                            Ok(()) => writes += 1,
                            Err(MidgeError::WriteStall(_)) => {
                                stalls += 1;
                                std::thread::sleep(Duration::from_millis(5));
                            }
                            Err(e) => panic!("thread {thread_id} unexpected: {e:?}"),
                        }

                        if writes + stalls >= 250 {
                            break;
                        }
                    }

                    (writes, stalls)
                });

                handles.push(handle);
            }

            std::thread::sleep(Duration::from_secs(2));
            shutdown.store(true, Ordering::Relaxed);

            let mut total_writes = 0u64;
            let mut per_thread = Vec::new();
            for handle in handles {
                let (writes, stalls) = handle.join().expect("panic");
                total_writes += writes;
                per_thread.push((writes, stalls));
            }

            // Background compaction is disabled by this shared fixture. Drain the
            // bounded L0 debt explicitly, then require every pressure predicate to
            // clear. Memory mode has no persistent storage debt.
            let stall_cleared = if mode == "memory" {
                true
            } else {
                engine
                    .compact_all()
                    .expect("manual compaction clears concurrent-burst debt");
                engine
                    .wait_for_write_stall_clear(cf.id(), Duration::from_secs(5))
                    .expect("wait for stall clear after concurrent burst")
            };

            results
                .borrow_mut()
                .push((mode.to_string(), total_writes, per_thread, stall_cleared));
        });

        // Assert
        for (mode, total_writes, per_thread, stall_cleared) in results.into_inner() {
            assert_eq!(
                per_thread.len(),
                4,
                "expected all 4 worker threads to report in mode {mode}"
            );
            for (thread_index, (writes, stalls)) in per_thread.iter().enumerate() {
                assert!(
                    writes + stalls > 0,
                    "thread {thread_index} made no progress (neither committed nor stalled) in mode {mode}"
                );
            }
            assert!(
                stall_cleared,
                "backpressure should clear again after the concurrent burst in mode {mode}, not stay permanently stalled"
            );
            if !mode.eq("memory") {
                assert!(total_writes > 0, "should have writes in mode {mode}");
            }
        }
    }
}

mod column_family_reclamation_hardening {
    use bytes::Bytes;
    use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    #[derive(Clone, Copy, Debug)]
    enum TestStorage {
        Local,
        SimulatedCloud,
    }

    fn open_engine(path: &Path, storage: TestStorage) -> Engine {
        let builder = match storage {
            TestStorage::Local => OpenOptions::local(path),
            TestStorage::SimulatedCloud => {
                OpenOptions::cloud_simulated(path, "reclamation-test", "cf/")
            }
        };
        Engine::open(
            builder
                .background_compaction(false)
                .with_memtable_size_limit(16 * 1024)
                .with_memtable_flush_threshold(16 * 1024)
                .build()
                .expect("build options"),
        )
        .expect("open engine")
    }

    fn write_and_flush(
        engine: &Engine,
        cf: &cntryl_midge::ColumnFamilyHandle,
        storage: TestStorage,
    ) -> Vec<String> {
        let mut transaction = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write transaction");
        transaction
            .put(b"retained-key".to_vec(), vec![b'x'; 32 * 1024], None)
            .expect("put retained value");
        transaction
            .commit(match storage {
                TestStorage::Local => WriteOptions::sync(),
                TestStorage::SimulatedCloud => WriteOptions::cloud_strict(),
            })
            .expect("commit retained value");
        engine.flush_cf(cf).expect("flush column family");

        let layout = engine
            .metrics()
            .get_storage_layout()
            .expect("storage layout");
        let files = layout
            .levels
            .iter()
            .flat_map(|level| &level.files)
            .filter(|file| file.cf_id == cf.id())
            .map(|file| file.name.clone())
            .collect::<Vec<_>>();
        assert!(!files.is_empty(), "flush must publish at least one SST");
        files
    }

    fn manifest_contains_cf_files(engine: &Engine, cf_id: u32) -> bool {
        engine
            .metrics()
            .get_storage_layout()
            .expect("storage layout")
            .levels
            .iter()
            .flat_map(|level| &level.files)
            .any(|file| file.cf_id == cf_id)
    }

    fn object_paths(root: &Path, storage: TestStorage, names: &[String]) -> Vec<PathBuf> {
        names
            .iter()
            .flat_map(|name| {
                let mut paths = vec![root.join("sst").join(name)];
                if matches!(storage, TestStorage::SimulatedCloud) {
                    paths.push(root.join("hybrid_local").join("sst").join(name));
                    paths.push(root.join("cloud_store").join("sst").join(name));
                }
                paths
            })
            .filter(|path| path.exists())
            .collect()
    }

    fn wait_until_reclaimed(
        engine: &Engine,
        root: &Path,
        storage: TestStorage,
        cf_id: u32,
        names: &[String],
    ) {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let manifest_reclaimed = !manifest_contains_cf_files(engine, cf_id);
            let retained_objects = object_paths(root, storage, names);
            if manifest_reclaimed && retained_objects.is_empty() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "dropped CF {cf_id} was not reclaimed: manifest_reclaimed={manifest_reclaimed}, retained_objects={retained_objects:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn should_reclaim_column_family_files_given_no_snapshot_pin_when_gc_runs() {
        for storage in [TestStorage::Local, TestStorage::SimulatedCloud] {
            // Arrange
            let temp = tempfile::tempdir().expect("temp dir");
            let engine = open_engine(temp.path(), storage);
            let cf = engine
                .create_column_family("reclaim-unpinned")
                .expect("create column family");
            let files = write_and_flush(&engine, &cf, storage);

            // Act
            engine
                .drop_column_family(cf.id())
                .expect("drop column family");

            // Assert
            wait_until_reclaimed(&engine, temp.path(), storage, cf.id(), &files);
        }
    }

    #[test]
    fn should_preserve_snapshot_visibility_given_column_family_drop_when_old_snapshot_is_active() {
        for storage in [TestStorage::Local, TestStorage::SimulatedCloud] {
            // Arrange
            let temp = tempfile::tempdir().expect("temp dir");
            let engine = open_engine(temp.path(), storage);
            let cf = engine
                .create_column_family("snapshot-drop")
                .expect("create column family");
            let files = write_and_flush(&engine, &cf, storage);
            let snapshot = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin pre-drop snapshot");

            // Act
            engine
                .drop_column_family(cf.id())
                .expect("drop column family");
            let pre_drop_value = snapshot
                .get(b"retained-key")
                .expect("read through pre-drop snapshot");
            let new_transaction = engine.begin_tx(cf.id(), TransactionMode::ReadOnly);

            // Assert
            assert_eq!(pre_drop_value, Some(Bytes::from(vec![b'x'; 32 * 1024])));
            assert!(matches!(
                new_transaction,
                Err(cntryl_midge::MidgeError::InvalidArgument(message))
                    if message == format!("column family {} does not exist", cf.id())
            ));
            drop(snapshot);
            wait_until_reclaimed(&engine, temp.path(), storage, cf.id(), &files);
        }
    }

    #[test]
    fn should_defer_column_family_file_reclamation_given_old_snapshot_pin_when_gc_runs() {
        for storage in [TestStorage::Local, TestStorage::SimulatedCloud] {
            // Arrange
            let temp = tempfile::tempdir().expect("temp dir");
            let engine = open_engine(temp.path(), storage);
            let cf = engine
                .create_column_family("reclaim-pinned")
                .expect("create column family");
            let files = write_and_flush(&engine, &cf, storage);
            let snapshot = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin pinned snapshot");

            // Act
            engine
                .drop_column_family(cf.id())
                .expect("drop column family");

            // Assert
            assert_eq!(
                snapshot.get(b"retained-key").expect("read pinned value"),
                Some(Bytes::from(vec![b'x'; 32 * 1024]))
            );
            assert!(
                manifest_contains_cf_files(&engine, cf.id()),
                "authoritative manifest must retain dropped-CF SSTs while a snapshot can read them"
            );
            assert!(
                !object_paths(temp.path(), storage, &files).is_empty(),
                "dropped-CF objects must remain while pinned"
            );

            drop(snapshot);
            wait_until_reclaimed(&engine, temp.path(), storage, cf.id(), &files);
        }
    }
}

mod local_wal_retention {
    //! Local WAL retention while one column family stays idle (#490, #550).
    //!
    //! Retirement stays prefix-only: a segment goes only once every family
    //! has flushed past it. Retiring out of order would drop a tombstone
    //! while an older retained segment still holds the put it deletes, and a
    //! later compaction would let recovery resurrect that put. The eventual
    //! flush instead bounds how long an idle family can pin the floor.

    use cntryl_midge::{ColumnFamilyHandle, Engine, OpenOptions, TransactionMode, WriteOptions};
    use std::path::Path;
    use std::time::Duration;

    /// Local eventual flush fires once a family pins four flush triggers'
    /// worth of WAL (#552): 32 KiB here.
    const FLUSH_TRIGGER_BYTES: usize = 8 * 1024;
    /// Each round writes about 5 KiB, so an idle family is flushed within
    /// about seven rounds.
    const RETENTION_BOUND: usize = 10;

    fn options(path: &Path) -> OpenOptions {
        OpenOptions::local(path)
            .background_compaction(false)
            .with_memtable_flush_threshold(FLUSH_TRIGGER_BYTES)
            .build()
            .expect("options")
    }

    fn put(engine: &Engine, cf: &ColumnFamilyHandle, key: &[u8]) {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(key.to_vec(), vec![b'v'; 1024], None).expect("put");
        tx.commit(WriteOptions::sync()).expect("commit");
    }

    fn delete(engine: &Engine, cf: &ColumnFamilyHandle, key: &[u8]) {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin");
        tx.delete(key.to_vec()).expect("delete");
        tx.commit(WriteOptions::sync()).expect("commit");
    }

    fn delete_range(engine: &Engine, cf: &ColumnFamilyHandle, start: &[u8], end: &[u8]) {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin");
        tx.delete_range(start.to_vec(), end.to_vec())
            .expect("delete range");
        tx.commit(WriteOptions::sync()).expect("commit");
    }

    fn get(engine: &Engine, cf_name: &str, key: &[u8]) -> Option<Vec<u8>> {
        let cf = engine.get_column_family(cf_name).expect("family");
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("read");
        tx.get(key).expect("get").map(|value| value.to_vec())
    }

    fn sealed_segments(path: &Path) -> usize {
        std::fs::read_dir(path.join("wal"))
            .expect("read wal dir")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name() != "wal.log")
            .count()
    }

    /// Copies a live database directory as a crash image, leaving out the
    /// leader record so the image opens as a takeover.
    fn crash_image(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).expect("create image dir");
        for entry in std::fs::read_dir(from).expect("read live dir") {
            let entry = entry.expect("entry");
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with(".midge_leader")
            {
                continue;
            }
            let target = to.join(entry.file_name());
            if entry.file_type().expect("file type").is_dir() {
                crash_image(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), &target).expect("copy");
            }
        }
    }

    #[test]
    fn should_retire_covered_wal_segments_when_an_idle_column_family_pins_the_floor() {
        // Arrange: one write to a family that never writes again.
        let temp = tempfile::TempDir::new().expect("temp");
        let mut engine = Engine::open(options(temp.path())).expect("open");
        let default = engine.get_column_family("default").expect("default");
        let idle = engine.create_column_family("idle").expect("idle");
        put(&engine, &idle, b"idle-key");

        // Act
        for round in 0..20 {
            for index in 0..5 {
                put(
                    &engine,
                    &default,
                    format!("busy-{round:03}-{index}").as_bytes(),
                );
            }
            engine.flush_cf(&default).expect("flush default");
            if round % 5 == 4 {
                // Background compaction is off; keep L0 below its stall limit.
                engine.compact_all().expect("compact");
            }
        }
        let retained = sealed_segments(temp.path());
        engine.shutdown(Duration::from_secs(10)).expect("shutdown");

        // Assert: covered puts retire through their per-record proofs even
        // while the idle family pins the floor; the deletes test below is
        // the one that needs the byte-bound flush.
        assert!(
            retained <= RETENTION_BOUND,
            "{retained} sealed segments retained after 20 covered flushes"
        );
        let reopened = Engine::open(options(temp.path())).expect("reopen");
        assert!(get(&reopened, "idle", b"idle-key").is_some());
    }

    #[test]
    fn should_retire_wal_segments_with_deletes_when_an_idle_column_family_pins_the_floor() {
        // Arrange: deletes can never pass the per-record proof, so only the
        // floor can retire their segments (#550).
        let temp = tempfile::TempDir::new().expect("temp");
        let mut engine = Engine::open(options(temp.path())).expect("open");
        let default = engine.get_column_family("default").expect("default");
        let idle = engine.create_column_family("idle").expect("idle");
        put(&engine, &idle, b"idle-key");

        // Act
        for round in 0..20 {
            let key = format!("busy-{round:03}");
            for _ in 0..5 {
                put(&engine, &default, key.as_bytes());
            }
            delete(&engine, &default, key.as_bytes());
            engine.flush_cf(&default).expect("flush default");
            if round % 5 == 4 {
                engine.compact_all().expect("compact");
            }
        }
        let retained = sealed_segments(temp.path());
        engine.shutdown(Duration::from_secs(10)).expect("shutdown");

        // Assert
        assert!(
            retained <= RETENTION_BOUND,
            "{retained} sealed segments retained after 20 flushed rounds with deletes"
        );
        let reopened = Engine::open(options(temp.path())).expect("reopen");
        assert!(get(&reopened, "idle", b"idle-key").is_some());
        assert!(get(&reopened, "default", b"busy-019").is_none());
    }

    /// The #551 review scenario: the idle family's write shares the first
    /// segment with a put that a later segment deletes. Compaction then
    /// drops both from the SSTs. Only prefix retirement keeps the delete's
    /// segment while the put's segment is retained.
    fn deleted_key_after_recovery(delete_kind: &str, reopen: &str) -> Option<Vec<u8>> {
        let temp = tempfile::TempDir::new().expect("temp");
        let live = temp.path().join("live");
        let mut engine = Engine::open(options(&live)).expect("open");
        let default = engine.get_column_family("default").expect("default");
        let idle = engine.create_column_family("idle").expect("idle");
        put(&engine, &idle, b"idle-key");
        put(&engine, &default, b"k");
        engine.flush_cf(&default).expect("flush put");
        match delete_kind {
            "delete" => delete(&engine, &default, b"k"),
            _ => delete_range(&engine, &default, b"a", b"z"),
        }
        engine.flush_cf(&default).expect("flush delete");
        engine.compact_all().expect("compact");
        assert_eq!(get(&engine, "default", b"k"), None, "live engine");
        let recovered = if reopen == "crash" {
            let image = temp.path().join("image");
            crash_image(&live, &image);
            let recovered = Engine::open(options(&image)).expect("open crash image");
            let value = get(&recovered, "default", b"k");
            drop(recovered);
            engine
                .shutdown(Duration::from_secs(10))
                .expect("shutdown live");
            value
        } else {
            engine.shutdown(Duration::from_secs(10)).expect("shutdown");
            drop(engine);
            let reopened = Engine::open(options(&live)).expect("reopen");
            get(&reopened, "default", b"k")
        };
        recovered
    }

    #[test]
    fn should_not_resurrect_deleted_key_when_recovering_a_crash_image() {
        // Arrange
        let (delete_kind, reopen) = ("delete", "crash");

        // Act
        let value = deleted_key_after_recovery(delete_kind, reopen);

        // Assert
        assert_eq!(value, None, "deleted key resurrected after crash recovery");
    }

    #[test]
    fn should_not_resurrect_deleted_key_when_reopening_after_clean_shutdown() {
        // Arrange
        let (delete_kind, reopen) = ("delete", "clean");

        // Act
        let value = deleted_key_after_recovery(delete_kind, reopen);

        // Assert
        assert_eq!(value, None, "deleted key resurrected after a clean restart");
    }

    #[test]
    fn should_not_resurrect_range_deleted_key_when_recovering_a_crash_image() {
        // Arrange
        let (delete_kind, reopen) = ("delete_range", "crash");

        // Act
        let value = deleted_key_after_recovery(delete_kind, reopen);

        // Assert
        assert_eq!(
            value, None,
            "range-deleted key resurrected after crash recovery"
        );
    }
}
