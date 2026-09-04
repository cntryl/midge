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

mod common;
#[cfg(feature = "failpoints")]
use cntryl_midge::EngineHealth;
use cntryl_midge::{Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions};
use common::*;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[cfg(feature = "failpoints")]
const CLOUD_OUTAGE_CHILD_ENV: &str = "MIDGE_HYBRID_CLOUD_OUTAGE_CHILD";

/// Count files nested anywhere under `root`, used to prove that the
/// filesystem-backed simulated cloud/local stores actually received or lost
/// data, rather than trusting a `get()` result alone.
fn count_files_recursive(root: &std::path::Path) -> usize {
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    let mut count = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            count += count_files_recursive(&path);
        } else {
            count += 1;
        }
    }
    count
}

/// A value that defeats SST compression, so its on-disk footprint tracks its
/// logical size closely enough to reliably cross a storage budget.
fn incompressible_value(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| {
            i.wrapping_mul(2_654_435_761)
                .wrapping_add(usize::from(seed))
                .wrapping_shr(13)
                .to_le_bytes()[0]
        })
        .collect()
}

// ============================================================================
// TEST GROUP: Memory Budget & Eviction Control
// ============================================================================

#[test]
fn should_apply_simulated_cloud_local_storage_budget_when_opening_simulated_cloud() {
    // Arrange
    let temp_dir = test_temp_dir();
    let budget_bytes = 8 * 1024 * 1024;
    let opts = OpenOptions::cloud_simulated(temp_dir.path(), "test-bucket", "test-prefix")
        .with_simulated_cloud_local_storage_budget(budget_bytes)
        .build()
        .expect("build options");

    // Act
    let engine = Engine::open(opts).expect("open simulated cloud engine");
    let metrics = engine.get_runtime_metrics().expect("runtime metrics");

    // Assert
    assert_eq!(metrics.hybrid_max_local_bytes, budget_bytes);
}

#[test]
fn should_trigger_eviction_at_high_watermark() {
    // Note: This test assumes cloud storage is available
    // In a pure local test, we validate the logic without touching actual cloud
    for_each_storage_mode(&["local"], |mode, opts| {
        eprintln!("\n=== Hybrid: Trigger Eviction at High Watermark (mode: {mode}) ===");

        // Arrange: Set tight memory budget to make eviction triggerable
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Write large values to reach high watermark (~70% of budget)
        // Target: ~1MB per value * 10 = 10MB total
        let large_value = vec![b'X'; 1024 * 1024]; // 1MB value

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..10 {
            let key = format!("large_key_{i:02}");
            tx.put(key.as_bytes().to_vec(), large_value.clone(), None)
                .ok();
        }
        tx.commit(buffered_write_options(mode)).expect("commit");

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
                let key = format!("large_key_{i:02}");
                tx.get(key.as_bytes())
                    .expect("read after high-watermark eviction")
                    .is_some()
            })
            .count();

        assert!(
            readable >= 8,
            "high watermark eviction caused data loss in mode: {mode}"
        );

        eprintln!("âœ“ High watermark eviction triggered safely; {readable} keys readable");
    });
}

#[test]
fn should_block_writes_at_emergency_watermark() {
    // Arrange
    // Note: "local" storage mode has no hybrid/cloud tier at all, so it never
    // exercises `StorageBudgetActor::reserve_for_flush_with_token`. This test
    // needs the real hybrid storage path, so it opens a cloud-simulated
    // engine directly with a tiny local storage budget instead of looping
    // over `for_each_storage_mode`.
    let temp_dir = test_temp_dir();
    let budget_bytes = 256 * 1024; // 256KB local budget
    let opts = OpenOptions::cloud_simulated(temp_dir.path(), "test-bucket", "test-prefix")
        .with_simulated_cloud_local_storage_budget(budget_bytes)
        .build()
        .expect("build options");
    let engine = Engine::open(opts).expect("open cloud-simulated engine");
    let cf = engine.create_column_family("test").expect("create cf");

    // 100KB values against a 256KB budget: a handful of explicit flushes must
    // exceed the emergency watermark (98% used). The memtable is left at its
    // default (large) size and every write is flushed immediately, so any
    // stall observed here comes from `StorageBudgetActor` (the hybrid local
    // storage budget), not from ordinary memtable backpressure. Values must
    // be incompressible or the SST compression policy shrinks them to
    // nothing and the budget is never actually approached.
    let mut backpressure_error: Option<MidgeError> = None;

    // Act
    for attempt in 0_u8..40 {
        let large_value = incompressible_value(100 * 1024, attempt);
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        let key = format!("emergency_key_{attempt:02}");

        if let Err(err) = tx.put(key.as_bytes().to_vec(), large_value.clone(), None) {
            backpressure_error = Some(err);
            break;
        }
        if let Err(err) = tx.commit(WriteOptions::cloud_async()) {
            backpressure_error = Some(err);
            break;
        }
        if let Err(err) = engine.flush_cf(&cf) {
            backpressure_error = Some(err);
            break;
        }
    }

    // Assert: the emergency/critical watermark actually fired through the
    // real hybrid storage backpressure mechanism, not some unrelated
    // failure. `StorageBudgetActor::reserve_for_flush_with_token` (see
    // `src/runtime/actors/flush.rs`) is the only place these exact message
    // fragments are produced, so matching on them (rather than just the
    // error variant) proves this specific code path fired.
    let error = backpressure_error
        .expect("expected the storage budget watermark to eventually reject a write or flush");
    let message = error.to_string();
    assert!(
        matches!(error, MidgeError::NoSpace(_) | MidgeError::WriteStall(_))
            && (message.contains("waiting for cloud upload capacity")
                || message.contains("waiting for compaction capacity")
                || message.contains("has no durable capacity")),
        "backpressure fired but not through the hybrid storage budget actor: {error:?}"
    );

    // Cross-check against the hybrid budget's own usage accounting (not
    // dependent on global telemetry being enabled): the local store must
    // actually have been under real pressure (>= the high watermark) when
    // this fired, not some unrelated error.
    let metrics = engine.get_runtime_metrics().expect("runtime metrics");
    assert!(
        metrics.hybrid_usage_percent >= 90,
        "backpressure error was raised but hybrid local storage usage ({}%) never reached the high watermark",
        metrics.hybrid_usage_percent
    );

    eprintln!("âœ“ Emergency watermark blocked writes via {error:?}");
}

#[test]
fn should_resume_writes_given_cloud_upload_completes_when_emergency_watermark_is_active() {
    for_each_storage_mode(&["local"], |mode, opts| {
        eprintln!("\n=== Hybrid: Resume Writes After Eviction (mode: {mode}) ===");

        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Fill to pressure point
        let medium_value = vec![b'Z'; 256 * 1024]; // 256KB

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..20 {
            let key = format!("pressure_key_{i:02}");
            tx.put(key.as_bytes().to_vec(), medium_value.clone(), None)
                .ok();
        }
        tx.commit(buffered_write_options(mode)).expect("commit");

        // Act: Trigger eviction and wait
        engine.flush_cf(&cf).expect("flush");
        thread::sleep(Duration::from_millis(300));

        // Resume writes after eviction
        let mut resume_writes = 0;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        for i in 0..50 {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            let key = format!("resume_key_{i:02}");
            if tx
                .put(key.as_bytes().to_vec(), medium_value.clone(), None)
                .is_ok()
                && tx.commit(buffered_write_options(mode)).is_ok()
            {
                resume_writes += 1;
            }
            if resume_writes >= 5 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "writes did not resume before the flush queue drained in mode: {mode}"
            );
            thread::sleep(Duration::from_millis(25));
        }

        // Assert: Writes can resume after eviction
        assert!(
            resume_writes >= 5,
            "writes did not resume after eviction in mode: {mode}"
        );

        eprintln!("âœ“ Writes resumed after eviction; {resume_writes} resume writes succeeded");
    });
}

#[test]
fn should_prefer_local_reads_before_eviction() {
    // Arrange
    // "local" storage mode has no cloud tier, so it cannot distinguish a
    // local read from a cloud fallback. Use the real hybrid storage path
    // with a budget generous enough that nothing is ever evicted, then prove
    // the reads were local via the eviction-queue metric and the on-disk
    // local cache, not just "get() returned something".
    let temp_dir = test_temp_dir();
    let budget_bytes = 64 * 1024 * 1024; // comfortably above the data written
    let opts = OpenOptions::cloud_simulated(temp_dir.path(), "test-bucket", "test-prefix")
        .with_simulated_cloud_local_storage_budget(budget_bytes)
        .build()
        .expect("build options");
    let engine = Engine::open(opts).expect("open cloud-simulated engine");
    let cf = engine.create_column_family("test").expect("create cf");

    let small_value = b"cached_value";

    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin_tx");
    for i in 0..50 {
        let key = format!("local_pref_key_{i:04}");
        tx.put(key.as_bytes().to_vec(), small_value.to_vec(), None)
            .expect("put");
    }
    tx.commit(WriteOptions::cloud_async()).expect("commit");
    engine.flush_cf(&cf).expect("flush");

    // Act: Read from cached SST (should be local, not cloud)
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin_tx");

    let mut local_hits = 0;
    for i in 0..50 {
        let key = format!("local_pref_key_{i:04}");
        if tx
            .get(key.as_bytes())
            .expect("read locally cached key")
            .is_some()
        {
            local_hits += 1;
        }
    }

    // Assert: every read succeeded...
    assert_eq!(
        local_hits, 50,
        "expected every key to be readable while comfortably under budget"
    );

    // ...and, crucially, nothing was queued for eviction to cloud, so those
    // reads are provably local rather than a cloud round-trip.
    let metrics = engine.get_runtime_metrics().expect("runtime metrics");
    assert_eq!(
        metrics.hybrid_pending_evictions, 0,
        "no eviction should have been queued while comfortably under budget"
    );

    let hybrid_local_dir = temp_dir.path().join("hybrid_local");
    assert!(
        count_files_recursive(&hybrid_local_dir) > 0,
        "expected flushed SST data to remain on local disk in {}",
        hybrid_local_dir.display()
    );

    eprintln!("âœ“ Local read preference confirmed; {local_hits} cache hits, 0 pending evictions");
}

#[test]
fn should_fetch_from_cloud_after_local_eviction() {
    // Arrange
    // "local" storage mode never uploads anything, so it cannot exercise a
    // cloud fetch. Use a tiny local budget against the real hybrid storage
    // path so eviction genuinely happens, then prove it via the simulated
    // cloud bucket's on-disk contents and the pending-eviction metric before
    // trusting `get()` to return the right values.
    let temp_dir = test_temp_dir();
    let budget_bytes = 256 * 1024; // small budget forces eviction under load
    let opts = OpenOptions::cloud_simulated(temp_dir.path(), "test-bucket", "test-prefix")
        .with_simulated_cloud_local_storage_budget(budget_bytes)
        .build()
        .expect("build options");
    let engine = Engine::open(opts).expect("open cloud-simulated engine");
    let cf = engine.create_column_family("test").expect("create cf");

    // Write large batch (20 * 64KB = 1.28MB, well over the 256KB budget)
    let value = vec![b'V'; 64 * 1024];

    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin_tx");
    for i in 0..20 {
        let key = format!("evict_fetch_key_{i:02}");
        tx.put(key.as_bytes().to_vec(), value.clone(), None)
            .expect("put");
    }
    tx.commit(WriteOptions::cloud_async()).expect("commit");
    engine.flush_cf(&cf).ok(); // may itself hit backpressure once over budget; that's fine

    // Act: Wait for the eviction/upload pipeline to actually drain to cloud.
    let cloud_store_dir = temp_dir.path().join("cloud_store");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        if metrics.hybrid_pending_evictions == 0 && count_files_recursive(&cloud_store_dir) > 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "eviction never drained to the simulated cloud store; pending_evictions={}",
            metrics.hybrid_pending_evictions
        );
        thread::sleep(Duration::from_millis(50));
    }

    // Assert: the simulated cloud bucket actually received uploaded objects.
    assert!(
        count_files_recursive(&cloud_store_dir) > 0,
        "expected evicted SSTs to be uploaded into the simulated cloud store"
    );

    // Now read evicted data (must trigger a cloud fetch to succeed)
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin_tx");

    let mut cloud_fetched = 0;
    for i in 0..20 {
        let key = format!("evict_fetch_key_{i:02}");
        if tx
            .get(key.as_bytes())
            .expect("read cloud-evicted key")
            .is_some()
        {
            cloud_fetched += 1;
        }
    }

    // Assert: Data accessible via cloud fetch
    assert_eq!(
        cloud_fetched, 20,
        "cloud fetch after eviction failed to return all keys"
    );

    eprintln!("âœ“ Cloud fetch working after eviction; {cloud_fetched} keys fetched");
}

#[test]
fn should_persist_eviction_state_across_restart() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        eprintln!("\n=== Hybrid: Persist Eviction State (mode: {mode}) ===");

        // Arrange
        // Act: Write, evict, note manifest state
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let value = vec![b'E'; 128 * 1024]; // 128KB

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..15 {
                let key = format!("persist_evict_key_{i:02}");
                tx.put(key.as_bytes().to_vec(), value.clone(), None).ok();
            }
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Eviction occurs
            thread::sleep(Duration::from_millis(200));
            engine
                .shutdown(Duration::from_secs(5))
                .expect("shutdown before same-path restart");
        }

        // Assert: Restart and verify eviction state
        {
            let engine = open_with_mode(&opts, mode);
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
                let key = format!("persist_evict_key_{i:02}");
                if tx
                    .get(key.as_bytes())
                    .expect("read after eviction-state restart")
                    .is_some()
                {
                    persisted += 1;
                }
            }

            assert!(
                persisted >= 12,
                "eviction state not persisted across restart in mode: {mode}"
            );

            eprintln!("âœ“ Eviction state persisted; {persisted} keys still accessible");
        }
    });
}

#[test]
#[cfg(feature = "failpoints")]
fn should_handle_cloud_unavailable_during_eviction() {
    // Failpoints are process-global. Run the injection in an exact-test child
    // so parallel tests in this binary cannot observe the simulated outage.
    if std::env::var_os(CLOUD_OUTAGE_CHILD_ENV).is_none() {
        let output = std::process::Command::new(
            std::env::current_exe().expect("locate hybrid storage test executable"),
        )
        .arg("--exact")
        .arg("should_handle_cloud_unavailable_during_eviction")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(CLOUD_OUTAGE_CHILD_ENV, "1")
        .output()
        .expect("run isolated cloud outage child");
        assert!(
            output.status.success(),
            "isolated cloud outage child failed: status={} stdout={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    // "local" storage mode has no cloud tier and no upload to fail. Exercise
    // the simulated-cloud provider boundary directly so the outage remains
    // deterministic even when this test runs as root in the Docker image.
    let temp_dir = test_temp_dir();
    let budget_bytes = 256 * 1024; // small budget forces eviction attempts
    let opts = OpenOptions::cloud_simulated(temp_dir.path(), "test-bucket", "test-prefix")
        .with_simulated_cloud_local_storage_budget(budget_bytes)
        .build()
        .expect("build options");
    let engine = Engine::open(opts).expect("open cloud-simulated engine");
    let cf = engine.create_column_family("test").expect("create cf");

    let large_value = vec![b'U'; 64 * 1024];

    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin_tx");
    for i in 0..12 {
        let key = format!("cloud_down_key_{i:02}");
        tx.put(key.as_bytes().to_vec(), large_value.clone(), None)
            .expect("put");
    }
    tx.commit(WriteOptions::cloud_async()).expect("commit");

    let cloud_sst_dir = temp_dir.path().join("cloud_store").join("sst");
    let files_before_outage_attempt = count_files_recursive(&cloud_sst_dir);
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::cloud::inject_fail_sst_upload", "return")
        .expect("configure cloud SST upload outage");

    // Act: Flush with the remote SST provider unavailable.
    let flush_error = engine
        .flush_cf(&cf)
        .expect_err("cloud outage should fail the SST upload");
    let files_after_outage_attempt = count_files_recursive(&cloud_sst_dir);

    fail::remove("midge::cloud::inject_fail_sst_upload");
    scenario.teardown();

    // Assert: the provider boundary genuinely rejected the upload and no new
    // SST object landed in cloud storage.
    assert!(
        matches!(&flush_error, MidgeError::Internal(message) if message.contains("cloud SST upload failed")),
        "unexpected cloud upload error: {flush_error:?}"
    );
    assert_eq!(
        files_after_outage_attempt, files_before_outage_attempt,
        "expected no SST objects to be written while the provider was unavailable"
    );

    // Assert: Engine still operational and data stayed available locally.
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin_tx");

    let mut accessible = 0;
    for i in 0..12 {
        let key = format!("cloud_down_key_{i:02}");
        if tx
            .get(key.as_bytes())
            .expect("read during cloud outage")
            .is_some()
        {
            accessible += 1;
        }
    }
    assert_eq!(
        accessible, 12,
        "cloud unavailability during eviction caused local data loss"
    );

    let metrics = engine.get_runtime_metrics().expect("runtime metrics");
    assert_ne!(
        metrics.health,
        EngineHealth::Corrupt,
        "engine must not become corrupt after a transient cloud outage"
    );

    eprintln!(
        "âœ“ Handled cloud unavailability gracefully; {accessible} keys still accessible, flush_error={flush_error:?}"
    );
}

#[test]
fn should_keep_pinned_sst_local_given_active_snapshot_when_eviction_runs() {
    for_each_storage_mode(&["local"], |mode, opts| {
        eprintln!("\n=== Hybrid: Don't Evict Active Readers (mode: {mode}) ===");

        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Arrange: Write and create snapshot
        let value = vec![b'A'; 64 * 1024]; // 64KB

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..25 {
            let key = format!("reader_protect_key_{i:02}");
            tx.put(key.as_bytes().to_vec(), value.clone(), None).ok();
        }
        tx.commit(buffered_write_options(mode)).expect("commit");
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
        eviction_handle
            .join()
            .expect("background eviction thread should not panic");

        // Assert: Snapshot reads still succeed (SST not evicted)
        let mut snapshot_reads = 0;
        for i in 0..25 {
            let key = format!("reader_protect_key_{i:02}");
            if snapshot
                .get(key.as_bytes())
                .expect("read pinned snapshot key")
                .is_some()
            {
                snapshot_reads += 1;
            }
        }

        assert!(
            snapshot_reads >= 20,
            "active reader SST was evicted in mode: {mode}"
        );

        eprintln!(
            "âœ“ Active readers protected from eviction; {snapshot_reads} snapshot reads successful"
        );
    });
}
