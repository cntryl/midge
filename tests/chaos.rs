//! Clean Shutdown Reopen Stress Tests
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
//!   should_<behavior>_given_<context>_when_<condition>

mod common;
use cntryl_midge::{TransactionMode, WriteOptions};
use common::*;
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
            let engine = open_with_mode(&opts, mode);
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
            let engine = open_with_mode(&opts, mode);
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
            let engine = open_with_mode(&opts, mode);
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
            let engine = open_with_mode(&opts, mode);
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
            let engine = open_with_mode(&opts, mode);
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
            let engine = open_with_mode(&opts, mode);
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
                            let key = format!("chaos_load_t{tid}_b{batch}_k{i:03}");
                            t.put(key.as_bytes().to_vec(), b"chaos_value".to_vec(), None)
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
                let key = format!("chaos_load_t{tid}_b{batch}_k000");
                if let Some(val) = tx.get(key.as_bytes()).expect("get") {
                    assert_eq!(val.as_ref(), b"chaos_value", "mode: {mode} key: {key}");
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
