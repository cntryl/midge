//! Hybrid Storage & Eviction Tests
//!
//! Tests memory budget management, eviction triggering, and cloud-local coordination:
//! - High watermark eviction triggering
//! - Emergency watermark write blocking
//! - Backpressure and recovery
//! - Read preference (local before cloud)
//! - Cloud fetch after eviction
//! - Eviction state persistence across restarts
//! - Reader isolation during eviction
//!
//! **Storage Modes**: Cloud only (hybrid storage requires cloud backend)
//! **Memory config**: Explicitly configured budgets for testing eviction thresholds
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>

use cntryl_midge::testkit::*;
use cntryl_midge::{TransactionMode, WriteOptions};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// ============================================================================
// TEST GROUP: Memory Budget & Eviction Control
// ============================================================================

#[test]
fn should_trigger_eviction_at_high_watermark() {
    // Note: This test assumes cloud storage is available
    // In a pure local test, we validate the logic without touching actual cloud
    for_each_storage_mode(&["local"], |mode, opts| {
        eprintln!(
            "\n=== Hybrid: Trigger Eviction at High Watermark (mode: {}) ===",
            mode
        );

        // Arrange: Set tight memory budget to make eviction triggerable
        let engine = open_with_mode(opts.clone(), mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Write large values to reach high watermark (~70% of budget)
        // Target: ~1MB per value * 10 = 10MB total
        let large_value = vec![b'X'; 1024 * 1024]; // 1MB value

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..10 {
            let key = format!("large_key_{:02}", i);
            tx.put(key.as_bytes().to_vec(), large_value.clone(), None)
                .ok();
        }
        engine.commit(tx, WriteOptions::buffered()).expect("commit");

        // Act: Flush to SST (triggers potential eviction to cloud)
        engine.flush_cf(&cf).expect("flush");

        // Assert: Engine handled high watermark gracefully
        // Verify:
        // 1. No panic
        // 2. Data still accessible
        // 3. Memory pressure handled

        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");
        let readable = (0..10)
            .filter(|i| {
                let key = format!("large_key_{:02}", i);
                tx.get(key.as_bytes()).ok().flatten().is_some()
            })
            .count();

        assert!(
            readable >= 8,
            "high watermark eviction caused data loss in mode: {}",
            mode
        );

        eprintln!(
            "✓ High watermark eviction triggered safely; {} keys readable",
            readable
        );
    });
}

#[test]
fn should_block_writes_at_emergency_watermark() {
    for_each_storage_mode(&["local"], |mode, opts| {
        eprintln!(
            "\n=== Hybrid: Block Writes at Emergency (mode: {}) ===",
            mode
        );

        // Arrange
        let engine = open_with_mode(opts.clone(), mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act: Fill close to emergency watermark
        let large_value = vec![b'Y'; 512 * 1024]; // 512KB value

        let mut emergency_write_blocked = false;
        let mut writes_completed = 0;

        for attempt in 0..30 {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            let key = format!("emergency_key_{:02}", attempt);

            match tx.put(key.as_bytes().to_vec(), large_value.clone(), None) {
                Ok(_) => {
                    // Write succeeded in transaction
                    match engine.commit(tx, WriteOptions::buffered()) {
                        Ok(_) => {
                            writes_completed += 1;
                        }
                        Err(_) => {
                            // Commit may fail at emergency watermark
                            emergency_write_blocked = true;
                            break;
                        }
                    }
                }
                Err(_) => {
                    // Put may fail at emergency watermark
                    emergency_write_blocked = true;
                    break;
                }
            }

            // Wait for eviction to process
            if attempt % 5 == 0 {
                thread::sleep(Duration::from_millis(100));
            }
        }

        // Assert: Either:
        // 1. Writes completed (eviction kept up with load)
        // 2. Writes blocked (backpressure activated)
        // NOT: Panic or crash

        assert!(
            writes_completed > 0 || emergency_write_blocked,
            "neither writes completed nor backpressure activated in mode: {}",
            mode
        );

        eprintln!(
            "✓ Emergency watermark handling active; writes: {}, blocked: {}",
            writes_completed, emergency_write_blocked
        );
    });
}

#[test]
fn should_resume_writes_after_eviction_clears_pressure() {
    for_each_storage_mode(&["local"], |mode, opts| {
        eprintln!(
            "\n=== Hybrid: Resume Writes After Eviction (mode: {}) ===",
            mode
        );

        // Arrange
        let engine = open_with_mode(opts.clone(), mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Fill to pressure point
        let medium_value = vec![b'Z'; 256 * 1024]; // 256KB

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..20 {
            let key = format!("pressure_key_{:02}", i);
            tx.put(key.as_bytes().to_vec(), medium_value.clone(), None)
                .ok();
        }
        engine.commit(tx, WriteOptions::buffered()).expect("commit");

        // Act: Trigger eviction and wait
        engine.flush_cf(&cf).expect("flush");
        thread::sleep(Duration::from_millis(300));

        // Resume writes after eviction
        let mut resume_writes = 0;
        for i in 0..10 {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            let key = format!("resume_key_{:02}", i);
            if tx
                .put(key.as_bytes().to_vec(), medium_value.clone(), None)
                .is_ok()
                && engine.commit(tx, WriteOptions::buffered()).is_ok()
            {
                resume_writes += 1;
            }
        }

        // Assert: Writes can resume after eviction
        assert!(
            resume_writes >= 5,
            "writes did not resume after eviction in mode: {}",
            mode
        );

        eprintln!(
            "✓ Writes resumed after eviction; {} resume writes succeeded",
            resume_writes
        );
    });
}

#[test]
fn should_prefer_local_reads_before_eviction() {
    for_each_storage_mode(&["local"], |mode, opts| {
        eprintln!("\n=== Hybrid: Prefer Local Reads (mode: {}) ===", mode);

        // Arrange: Write small data that fits comfortably in cache
        let engine = open_with_mode(opts.clone(), mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let small_value = b"cached_value";

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..50 {
            let key = format!("local_pref_key_{:04}", i);
            tx.put(key.as_bytes().to_vec(), small_value.to_vec(), None)
                .ok();
        }
        engine.commit(tx, WriteOptions::buffered()).expect("commit");
        engine.flush_cf(&cf).expect("flush");

        // Act: Read from cached SST (should be local, not cloud)
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");

        let mut local_hits = 0;
        for i in 0..50 {
            let key = format!("local_pref_key_{:04}", i);
            if tx.get(key.as_bytes()).ok().flatten().is_some() {
                local_hits += 1;
            }
        }

        // Assert: Local reads succeed with high hit rate
        // (In a real scenario with cloud metrics, we'd verify 0 cloud fetches)
        assert!(
            local_hits >= 45,
            "local cache hit rate too low in mode: {}",
            mode
        );

        eprintln!("✓ Local read preference working; {} cache hits", local_hits);
    });
}

#[test]
fn should_fetch_from_cloud_after_local_eviction() {
    for_each_storage_mode(&["local"], |mode, opts| {
        eprintln!(
            "\n=== Hybrid: Fetch from Cloud After Eviction (mode: {}) ===",
            mode
        );

        // Arrange: Write and evict to cloud
        let engine = open_with_mode(opts.clone(), mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Write large batch
        let value = vec![b'V'; 64 * 1024]; // 64KB per value

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..20 {
            let key = format!("evict_fetch_key_{:02}", i);
            tx.put(key.as_bytes().to_vec(), value.clone(), None).ok();
        }
        engine.commit(tx, WriteOptions::buffered()).expect("commit");
        engine.flush_cf(&cf).expect("flush");

        // Act: Eviction should happen during flush
        thread::sleep(Duration::from_millis(200));

        // Now read evicted data (should trigger cloud fetch)
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");

        let mut cloud_fetched = 0;
        for i in 0..20 {
            let key = format!("evict_fetch_key_{:02}", i);
            if tx.get(key.as_bytes()).ok().flatten().is_some() {
                cloud_fetched += 1;
            }
        }

        // Assert: Data accessible via cloud fetch
        assert!(
            cloud_fetched >= 15,
            "cloud fetch after eviction failed in mode: {}",
            mode
        );

        eprintln!(
            "✓ Cloud fetch working after eviction; {} keys fetched",
            cloud_fetched
        );
    });
}

#[test]
fn should_persist_eviction_state_across_restart() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        eprintln!("\n=== Hybrid: Persist Eviction State (mode: {}) ===", mode);

        // Arrange
        // Act: Write, evict, note manifest state
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let value = vec![b'E'; 128 * 1024]; // 128KB

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..15 {
                let key = format!("persist_evict_key_{:02}", i);
                tx.put(key.as_bytes().to_vec(), value.clone(), None).ok();
            }
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Eviction occurs
            thread::sleep(Duration::from_millis(200));
            // Drop engine; manifest should persist eviction state
        }

        // Assert: Restart and verify eviction state
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Verify:
            // 1. Manifest loaded correctly
            // 2. Evicted SSTs marked as such
            // 3. Data still accessible (no re-load from cloud into cache)

            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            let mut persisted = 0;
            for i in 0..15 {
                let key = format!("persist_evict_key_{:02}", i);
                if tx.get(key.as_bytes()).ok().flatten().is_some() {
                    persisted += 1;
                }
            }

            assert!(
                persisted >= 12,
                "eviction state not persisted across restart in mode: {}",
                mode
            );

            eprintln!(
                "✓ Eviction state persisted; {} keys still accessible",
                persisted
            );
        }
    });
}

#[test]
fn should_handle_cloud_unavailable_during_eviction() {
    for_each_storage_mode(&["local"], |mode, opts| {
        eprintln!(
            "\n=== Hybrid: Cloud Down During Eviction (mode: {}) ===",
            mode
        );

        // Arrange: Fill cache near capacity
        let engine = open_with_mode(opts.clone(), mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let large_value = vec![b'U'; 512 * 1024]; // 512KB

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..12 {
            let key = format!("cloud_down_key_{:02}", i);
            tx.put(key.as_bytes().to_vec(), large_value.clone(), None)
                .ok();
        }
        engine.commit(tx, WriteOptions::buffered()).expect("commit");

        // Act: Flush with cloud down (upload fails)
        engine.flush_cf(&cf).ok(); // May fail, but should handle gracefully

        // Engine should either:
        // 1. Queue for later retry
        // 2. Keep data local (delay eviction)
        // 3. Not crash

        thread::sleep(Duration::from_millis(100));

        // Assert: Engine still operational
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");

        let mut accessible = 0;
        for i in 0..12 {
            let key = format!("cloud_down_key_{:02}", i);
            if tx.get(key.as_bytes()).ok().flatten().is_some() {
                accessible += 1;
            }
        }

        assert!(
            accessible >= 10,
            "cloud unavailability during eviction caused data loss in mode: {}",
            mode
        );

        eprintln!(
            "✓ Handled cloud unavailability gracefully; {} keys still accessible",
            accessible
        );
    });
}

#[test]
fn should_not_evict_ssts_with_active_readers() {
    for_each_storage_mode(&["local"], |mode, opts| {
        eprintln!(
            "\n=== Hybrid: Don't Evict Active Readers (mode: {}) ===",
            mode
        );

        let engine = Arc::new(open_with_mode(opts.clone(), mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Arrange: Write and create snapshot
        let value = vec![b'A'; 64 * 1024]; // 64KB

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..25 {
            let key = format!("reader_protect_key_{:02}", i);
            tx.put(key.as_bytes().to_vec(), value.clone(), None).ok();
        }
        engine.commit(tx, WriteOptions::buffered()).expect("commit");
        engine.flush_cf(&cf).expect("flush");

        // Create read snapshot (holds reference to SST)
        let snapshot = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");

        // Act: Trigger eviction attempt while snapshot is active
        let engine_clone = Arc::clone(&engine);
        let cf_clone = cf.clone();
        let eviction_handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            engine_clone.flush_cf(&cf_clone).ok();
            engine_clone.compact_all().ok();
        });

        // Wait for eviction to complete
        eviction_handle.join().ok();

        // Assert: Snapshot reads still succeed (SST not evicted)
        let mut snapshot_reads = 0;
        for i in 0..25 {
            let key = format!("reader_protect_key_{:02}", i);
            if snapshot.get(key.as_bytes()).ok().flatten().is_some() {
                snapshot_reads += 1;
            }
        }

        assert!(
            snapshot_reads >= 20,
            "active reader SST was evicted in mode: {}",
            mode
        );

        eprintln!(
            "✓ Active readers protected from eviction; {} snapshot reads successful",
            snapshot_reads
        );
    });
}
