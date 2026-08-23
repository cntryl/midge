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
//!   should_<behavior>_given_<context>_when_<condition>

mod common;
use cntryl_midge::{TransactionMode, WriteOptions};
use common::*;
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
            retained_input_count, 1,
            "the configured three-file batch must reclaim exactly three physical inputs"
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
            let metrics = engine.get_runtime_metrics().expect("runtime metrics");
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
