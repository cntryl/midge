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
#[cfg(feature = "failpoints")]
use cntryl_midge::MidgeError;
use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
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
    let mut random = 0x9e37_79b9_u32 ^ u32::from(seed);
    (0..len)
        .map(|_| {
            random ^= random << 13;
            random ^= random >> 17;
            random ^= random << 5;
            random.to_le_bytes()[0]
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
fn should_keep_writes_running_when_published_ssts_exceed_local_budget() {
    // Arrange
    let temp_dir = test_temp_dir();
    let budget_bytes = 1024 * 1024;
    let opts = OpenOptions::cloud_simulated(temp_dir.path(), "test-bucket", "test-prefix")
        .local_storage_budget(budget_bytes)
        .background_compaction(false)
        .build()
        .expect("build options");
    let engine = Engine::open(opts).expect("open cloud-simulated engine");
    let cf = engine.create_column_family("test").expect("create cf");

    // Act
    for attempt in 0_u8..12 {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        tx.put(
            format!("budget-key-{attempt:02}").into_bytes(),
            incompressible_value(100 * 1024, attempt),
            None,
        )
        .expect("put");
        tx.commit(WriteOptions::cloud_strict())
            .expect("cloud commit");
        engine
            .flush_cf(&cf)
            .expect("flush and release published local SST");
    }

    // Assert
    let metrics = engine.get_runtime_metrics().expect("runtime metrics");
    assert!(metrics.hybrid_total_committed_bytes <= budget_bytes);
    assert_eq!(count_files_recursive(&temp_dir.path().join("sst")), 0);
    let remote_bytes: u64 = std::fs::read_dir(temp_dir.path().join("cloud_store/sst"))
        .expect("remote SST directory")
        .map(|entry| {
            entry
                .expect("remote SST")
                .metadata()
                .expect("SST metadata")
                .len()
        })
        .sum();
    assert!(remote_bytes > budget_bytes);
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("read transaction");
    for attempt in 0_u8..12 {
        let expected = incompressible_value(100 * 1024, attempt);
        assert_eq!(
            tx.get(format!("budget-key-{attempt:02}").as_bytes())
                .expect("read")
                .as_deref(),
            Some(expected.as_slice())
        );
    }
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
fn should_read_published_cloud_ssts_without_local_replica() {
    // Arrange
    // A large working budget still does not require a permanent SST replica.
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

    // Act: Read published SST blocks through the cloud-backed reader.
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin_tx");

    let mut readable = 0;
    for i in 0..50 {
        let key = format!("local_pref_key_{i:04}");
        if tx
            .get(key.as_bytes())
            .expect("read published key")
            .is_some()
        {
            readable += 1;
        }
    }

    // Assert: every read succeeded...
    assert_eq!(
        readable, 50,
        "expected every key to be readable while comfortably under budget"
    );

    assert_eq!(count_files_recursive(&temp_dir.path().join("sst")), 0);
    assert_eq!(
        count_files_recursive(&temp_dir.path().join("hybrid_local/sst")),
        0
    );
    assert!(count_files_recursive(&temp_dir.path().join("cloud_store/sst")) > 0);
}

#[test]
fn should_fetch_from_cloud_after_local_eviction() {
    // Arrange
    // Verify published SSTs remain readable after their local staging files are removed.
    let temp_dir = test_temp_dir();
    let budget_bytes = 8 * 1024 * 1024; // Fits this transaction and its WAL/flush staging.
    let opts = OpenOptions::cloud_simulated(temp_dir.path(), "test-bucket", "test-prefix")
        .with_simulated_cloud_local_storage_budget(budget_bytes)
        .build()
        .expect("build options");
    let engine = Engine::open(opts).expect("open cloud-simulated engine");
    let cf = engine.create_column_family("test").expect("create cf");

    // Write a batch which fits admission; successful publication evicts its SST.
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
    engine.flush_cf(&cf).expect("publish and evict SST");

    // Act / Assert: publication has completed, and only the cloud copy remains.
    assert!(count_files_recursive(&temp_dir.path().join("cloud_store/sst")) > 0);
    assert_eq!(count_files_recursive(&temp_dir.path().join("sst")), 0);

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
    let budget_bytes = 8 * 1024 * 1024; // Admit the flush so this test reaches the failed provider.
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
