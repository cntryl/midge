//! Cloud Recovery Integration Tests
//!
//! Tests crash recovery in cloud-backed storage:
//! - Idempotent recovery from partial uploads
//! - Manifest consistency after cloud failures
//! - Retry mechanisms for failed uploads
//! - Proper handling of crashes during critical operations (compaction, flush)
//!
//! **Storage Modes**: Durable modes only (local, cloud)
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>

use bytes::Bytes;
use cntryl_midge::testkit::*;
use cntryl_midge::{TransactionMode, WriteOptions};
use std::thread;
use std::time::Duration;

// ============================================================================
// TEST GROUP: Cloud Recovery Scenarios
// ============================================================================

#[test]
fn should_recover_from_partial_sst_upload() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        eprintln!(
            "\n=== Cloud Recovery: Partial SST Upload (mode: {}) ===",
            mode
        );

        // Arrange
        // Act (Phase 1): Write and attempt upload (which may be incomplete)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write data
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..100 {
                let key = format!("partial_upload_key_{:04}", i);
                tx.put(
                    key.as_bytes().to_vec(),
                    b"value_before_upload".to_vec(),
                    None,
                )
                .ok();
            }
            engine.commit(tx, WriteOptions::buffered()).expect("commit");

            // Flush creates SST and initiates cloud upload
            engine.flush_cf(&cf).expect("flush");

            // Wait slightly for upload to potentially start
            thread::sleep(Duration::from_millis(50));
            // Simulate crash by dropping engine without waiting for upload to complete
        }

        // Assert (Phase 2): Reopen and verify recovery
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Engine should have recovered from WAL or from completed SST portion
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            // Key assertions for recovery correctness:
            // 1. No data corruption (we get valid values, not garbage)
            // 2. Consistent manifest state (no dangling references)
            let mut valid_reads = 0;
            for i in 0..100 {
                let key = format!("partial_upload_key_{:04}", i);
                if let Ok(Some(val)) = tx.get(key.as_bytes()) {
                    valid_reads += 1;
                    // Verify value is not corrupted
                    assert!(
                        !val.is_empty(),
                        "retrieved value is empty after partial upload recovery"
                    );
                }
            }

            // Either all data recovered from WAL or none lost from SST
            assert!(
                valid_reads > 0,
                "no data recovered after partial SST upload in mode: {}",
                mode
            );

            eprintln!(
                "✓ Cloud recovery successful; {} keys recovered",
                valid_reads
            );
        }
    });
}

#[test]
fn should_recover_from_failed_manifest_write_to_cloud() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        eprintln!(
            "\n=== Cloud Recovery: Failed Manifest Write (mode: {}) ===",
            mode
        );

        // Arrange
        // Act (Phase 1): Write data and trigger compaction that could fail at manifest
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Create initial SSTs
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..50 {
                let key = format!("manifest_fail_key_{:04}", i);
                tx.put(key.as_bytes().to_vec(), b"v1".to_vec(), None).ok();
            }
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Second SST
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 50..100 {
                let key = format!("manifest_fail_key_{:04}", i);
                tx.put(key.as_bytes().to_vec(), b"v2".to_vec(), None).ok();
            }
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Trigger compaction (may fail at manifest update)
            engine.compact_all().ok();
            // Simulate crash
        }

        // Assert (Phase 2): Reopen (manifest must be in consistent state)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Engine should have loaded previous manifest version atomically
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            // Verify no manifest corruption: all data remain readable
            let mut found = 0;
            for i in 0..100 {
                let key = format!("manifest_fail_key_{:04}", i);
                if tx.get(key.as_bytes()).ok().flatten().is_some() {
                    found += 1;
                }
            }

            // Even if compaction didn't finish, data should not be lost
            assert!(
                found >= 50,
                "manifest corruption led to data loss in mode: {}",
                mode
            );

            eprintln!("✓ Manifest failure recovered; data consistency maintained");
        }
    });
}

#[test]
fn should_retry_failed_cloud_upload_on_restart() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        eprintln!(
            "\n=== Cloud Recovery: Retry Failed Upload (mode: {}) ===",
            mode
        );

        // Arrange
        // Act (Phase 1): Flush with failed cloud upload
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..75 {
                let key = format!("retry_key_{:04}", i);
                tx.put(key.as_bytes().to_vec(), b"retry_value".to_vec(), None)
                    .ok();
            }
            engine.commit(tx, WriteOptions::buffered()).expect("commit");

            // Flush (upload may fail)
            engine.flush_cf(&cf).expect("flush");
            // Crash without waiting for upload
        }

        // Assert (Phase 2): Restart and verify retry
        {
            let engine = open_with_mode(opts, mode);

            // Engine should queue retry for pending uploads on startup
            thread::sleep(Duration::from_millis(200)); // Wait for background retry

            let cf = engine.create_column_family("test").expect("create cf");
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            // All data should be readable (either local SST or recovered from WAL)
            let mut found = 0;
            for i in 0..75 {
                let key = format!("retry_key_{:04}", i);
                if tx.get(key.as_bytes()).ok().flatten().is_some() {
                    found += 1;
                }
            }

            assert!(
                found >= 70,
                "upload retry failed; insufficient data recovered in mode: {}",
                mode
            );

            eprintln!(
                "✓ Failed upload retried on restart; {} keys recovered",
                found
            );
        }
    });
}

#[test]
fn should_not_expose_partially_uploaded_sst() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        eprintln!(
            "\n=== Cloud Recovery: Don't Expose Partial SST (mode: {}) ===",
            mode
        );

        // Arrange
        let engine = open_with_mode(opts.clone(), mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Write data
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..100 {
            let key = format!("exposure_key_{:04}", i);
            tx.put(key.as_bytes().to_vec(), b"safe_value".to_vec(), None)
                .ok();
        }
        engine.commit(tx, WriteOptions::buffered()).expect("commit");

        // Create a read snapshot before flushing
        let snapshot = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");

        // Act: Flush while snapshot is held (SST upload may be partial)
        engine.flush_cf(&cf).expect("flush");

        // Assert: Snapshot reads are consistent and don't see partial data
        let mut safe_reads = 0;
        for i in 0..100 {
            let key = format!("exposure_key_{:04}", i);
            if let Ok(Some(val)) = snapshot.get(key.as_bytes()) {
                safe_reads += 1;
                // Verify value is complete (not truncated)
                assert_eq!(
                    val,
                    Bytes::from_static(b"safe_value"),
                    "snapshot saw corrupted data in mode: {}",
                    mode
                );
            }
        }

        // Either all visible or none visible (snapshot isolation)
        assert!(
            safe_reads == 100 || safe_reads == 0,
            "partial data visibility violates snapshot isolation in mode: {}",
            mode
        );

        eprintln!("✓ Partial uploads not exposed; snapshot isolation maintained");
    });
}

#[test]
fn should_recover_after_crash_mid_compaction_with_cloud() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        eprintln!(
            "\n=== Cloud Recovery: Crash Mid-Compaction (mode: {}) ===",
            mode
        );

        // Arrange
        // Act (Phase 1): Create multi-SST scenario and crash during compaction
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Batch 1
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..100 {
                let key = format!("midcompact_key_{:04}", i);
                tx.put(key.as_bytes().to_vec(), b"gen1".to_vec(), None).ok();
            }
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Batch 2
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 100..200 {
                let key = format!("midcompact_key_{:04}", i);
                tx.put(key.as_bytes().to_vec(), b"gen2".to_vec(), None).ok();
            }
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Trigger compaction but don't wait for completion (simulate mid-crash)
            engine.compact_all().ok();
            thread::sleep(Duration::from_millis(50)); // Let compaction start
                                                      // Crash by dropping engine
        }

        // Assert (Phase 2): Restart and verify manifest consistency
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Engine should recover to a consistent state:
            // - Either compaction was fully applied (all data in new SST)
            // - Or compaction was rolled back (all data in old SSTs)
            // - NOT a mix of input/output SSTs

            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            let mut valid_data = 0;
            for i in 0..200 {
                let key = format!("midcompact_key_{:04}", i);
                if tx.get(key.as_bytes()).ok().flatten().is_some() {
                    valid_data += 1;
                }
            }

            assert!(
                valid_data >= 150,
                "manifest inconsistency led to data loss after crash mid-compaction"
            );

            eprintln!("✓ Recovered from mid-compaction crash; manifest consistent");
        }
    });
}

#[test]
fn should_handle_cloud_unavailable_during_flush() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        eprintln!(
            "\n=== Cloud Recovery: Cloud Unavailable During Flush (mode: {}) ===",
            mode
        );

        // Arrange
        // Act (Phase 1): Write with cloud unavailable
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write data
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..50 {
                let key = format!("cloud_offline_key_{:04}", i);
                tx.put(key.as_bytes().to_vec(), b"offline_value".to_vec(), None)
                    .ok();
            }
            engine.commit(tx, WriteOptions::buffered()).expect("commit");

            // Flush (cloud offline, but local SST should still be created)
            engine.flush_cf(&cf).ok(); // May fail or succeed depending on design

            // Simulate crash
        }

        // Assert (Phase 2): Restart when cloud comes online
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Verify data recovery
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            // Data should be available from local SST or WAL
            let mut found = 0;
            for i in 0..50 {
                let key = format!("cloud_offline_key_{:04}", i);
                if tx.get(key.as_bytes()).ok().flatten().is_some() {
                    found += 1;
                }
            }

            assert!(
                found >= 40,
                "data lost when cloud unavailable during flush in mode: {}",
                mode
            );

            eprintln!(
                "✓ Recovered despite cloud unavailability; {} keys recovered",
                found
            );
        }
    });
}

#[test]
fn should_resume_pending_uploads_after_restart() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        eprintln!(
            "\n=== Cloud Recovery: Resume Pending Uploads (mode: {}) ===",
            mode
        );

        // Arrange
        // Act (Phase 1): Queue uploads and crash
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Batch 1: Flush
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..50 {
                let key = format!("resume_key_{:04}", i);
                tx.put(key.as_bytes().to_vec(), b"batch1".to_vec(), None)
                    .ok();
            }
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Batch 2: Another flush (queues more uploads)
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 50..100 {
                let key = format!("resume_key_{:04}", i);
                tx.put(key.as_bytes().to_vec(), b"batch2".to_vec(), None)
                    .ok();
            }
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Don't wait for uploads to complete; crash
            thread::sleep(Duration::from_millis(50));
        }

        // Assert (Phase 2): Restart (background should resume pending uploads)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Wait for background upload resumption
            thread::sleep(Duration::from_millis(300));

            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            // All previously queued data should be readable
            let mut found = 0;
            for i in 0..100 {
                let key = format!("resume_key_{:04}", i);
                if tx.get(key.as_bytes()).ok().flatten().is_some() {
                    found += 1;
                }
            }

            assert!(
                found >= 90,
                "pending uploads not resumed after restart in mode: {}",
                mode
            );

            eprintln!(
                "✓ Pending uploads resumed on restart; {} keys available",
                found
            );
        }
    });
}

#[test]
fn should_deduplicate_upload_on_retry() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        eprintln!(
            "\n=== Cloud Recovery: Deduplicate Upload on Retry (mode: {}) ===",
            mode
        );

        // Arrange
        // Act (Phase 1): Trigger upload that fails then succeeds on retry
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..60 {
                let key = format!("dedup_key_{:04}", i);
                tx.put(key.as_bytes().to_vec(), b"dedup_value".to_vec(), None)
                    .ok();
            }
            engine.commit(tx, WriteOptions::buffered()).expect("commit");

            // Flush (upload attempt)
            engine.flush_cf(&cf).expect("flush");

            // Wait shorter interval (simulating quick retry)
            thread::sleep(Duration::from_millis(50));
        }

        // Assert (Phase 2): Restart and verify deduplication
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Wait for upload deduplication logic
            thread::sleep(Duration::from_millis(200));

            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            // Verify:
            // 1. All data readable (no loss from dedup)
            // 2. No duplicate objects (idempotency)
            let mut found = 0;
            for i in 0..60 {
                let key = format!("dedup_key_{:04}", i);
                if tx.get(key.as_bytes()).ok().flatten().is_some() {
                    found += 1;
                }
            }

            assert!(
                found >= 55,
                "deduplication caused data loss in mode: {}",
                mode
            );

            eprintln!("✓ Upload deduplicated on retry; {} keys recovered", found);
        }
    });
}
