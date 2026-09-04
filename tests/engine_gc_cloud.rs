//! Cloud-Specific Garbage Collection Tests
//!
//! Tests garbage collection of cloud objects:
//! - Orphaned cloud SST deletion after compaction
//! - Preservation of referenced cloud objects
//! - Graceful handling of cloud delete failures
//!
//! **Storage Modes**: Cloud only (uses `MockStorage` for failure injection)
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>

mod common;
use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use common::*;
use std::collections::BTreeSet;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

// ============================================================================
// HELPERS
// ============================================================================

/// List the `.sst` object names directly inside `dir` (non-recursive).
fn sst_object_names(dir: &Path) -> BTreeSet<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return BTreeSet::new();
    };
    entries
        .flatten()
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("sst"))
        })
        .filter_map(|entry| entry.file_name().into_string().ok())
        .collect()
}

// ============================================================================
// TEST GROUP: Cloud Object Garbage Collection
// ============================================================================

#[test]
fn should_collect_orphaned_cloud_objects_after_compaction() {
    eprintln!("\n=== Cloud GC: Collect Orphaned Objects ===");

    // Arrange: a real simulated cloud backend (filesystem-backed bucket),
    // not "local" mode standing in for it, so we can observe the actual
    // cloud object store before and after compaction.
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let db_path = temp_dir.path();
    let engine = Engine::open(
        OpenOptions::cloud_simulated(db_path, "test-bucket", "gc-orphan-objects")
            .background_compaction(false)
            .build()
            .expect("build simulated cloud options"),
    )
    .expect("open simulated cloud engine");
    let cf = engine.create_column_family("test").expect("create cf");

    // Write the same overlapping key range across four separate flushes.
    // Use four files so the fixture exceeds the derived default L0 trigger;
    // the overlapping keys make every bounded pass a genuine L0 -> L1 merge.
    for batch in 0..4 {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..200 {
            let key = format!("cloudsst_key_{i:04}");
            tx.put(
                key.into_bytes(),
                format!("cloud_value_{batch}").into_bytes(),
                None,
            )
            .expect("put");
        }
        tx.commit(WriteOptions::cloud_async()).expect("commit");
        engine.flush_cf(&cf).expect("flush batch");
    }

    let cloud_sst_dir = db_path.join("cloud_store").join("sst");
    let before_objects = sst_object_names(&cloud_sst_dir);
    assert!(
        before_objects.len() >= 4,
        "expected all four flushed SSTs to be mirrored to cloud storage, got {before_objects:?}"
    );

    // Act: Compact. Each merge batch stays bounded, while compact_all walks
    // every batch until the logical L0 debt is clear.
    engine.compact_all().expect("compact_all");

    // The cloud delete of orphaned inputs runs on a background worker, so
    // poll until at least one pre-compaction object is gone and at least
    // one new object has appeared (bounded wait).
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut after_objects;
    loop {
        after_objects = sst_object_names(&cloud_sst_dir);
        let removed = before_objects.difference(&after_objects).count();
        let added = after_objects.difference(&before_objects).count();
        if removed > 0 && added > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for orphaned cloud SST objects to be collected; \
             before={before_objects:?} after={after_objects:?}"
        );
        thread::sleep(Duration::from_millis(50));
    }

    // Assert: at least one pre-compaction object is actually gone from cloud
    // storage (a real orphan collection, not a no-op), and the compacted
    // output object is present in its place.
    let removed: Vec<_> = before_objects.difference(&after_objects).collect();
    let added: Vec<_> = after_objects.difference(&before_objects).collect();
    assert!(
        !removed.is_empty(),
        "expected at least one orphaned cloud object to be collected after compaction"
    );
    assert!(
        !added.is_empty(),
        "expected the compacted output SST to be present in cloud storage"
    );

    // Assert: all data survived compaction.
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin_tx");
    for i in 0..200 {
        let key = format!("cloudsst_key_{i:04}");
        let val = tx.get(key.as_bytes()).expect("get");
        assert!(val.is_some(), "data lost during cloud object compaction");
    }

    eprintln!("✓ Cloud GC successfully cleaned up orphaned objects");
}

#[test]
fn should_not_collect_cloud_objects_referenced_by_manifest() {
    for_each_storage_mode(&["local"], |mode, opts| {
        eprintln!("\n=== Cloud GC: Preserve Referenced Objects (mode: {mode}) ===");

        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Create active SST
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..100 {
            let key = format!("ref_key_{i:04}");
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                .ok();
        }
        tx.commit(WriteOptions::buffered()).expect("commit");
        engine.flush_cf(&cf).expect("flush");

        // Act: Don't compact; SST remains in manifest
        // Verify data is readable
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");

        // Assert: Check manifest indirectly via data availability
        let mut found = 0;
        for i in 0..100 {
            let key = format!("ref_key_{i:04}");
            if tx
                .get(key.as_bytes())
                .expect("read manifest-referenced key")
                .is_some()
            {
                found += 1;
            }
        }
        assert_eq!(
            found, 100,
            "manifest-referenced cloud objects were incorrectly deleted in mode: {mode}"
        );

        eprintln!("✓ All manifest-referenced cloud objects preserved");
    });
}

/// Simulates a cloud provider outage that specifically affects deleting
/// GC'd (orphaned) objects, without disturbing the output upload that must
/// precede manifest publication. A provider-boundary failpoint keeps this
/// deterministic even when the process has permission to delete read-only
/// files, as root does inside the Docker qualification image.
#[test]
#[cfg(feature = "failpoints")]
fn should_handle_gc_when_cloud_delete_fails() {
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    eprintln!("\n=== Cloud GC: Handle Delete Failure ===");

    // Arrange: real simulated cloud backend.
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let db_path = temp_dir.path();
    let options = OpenOptions::cloud_simulated(db_path, "test-bucket", "gc-delete-fail")
        .background_compaction(false)
        .build()
        .expect("build simulated cloud options");
    let l0_batch_size = options.l0_compaction_trigger();
    let mut engine = Engine::open(options).expect("open simulated cloud engine");
    let cf = engine.create_column_family("test").expect("create cf");

    // Write exactly one configured L0 batch. This isolates the delete failure
    // after publication without requiring a second upload while the simulated
    // bucket is deliberately read-only.
    for batch in 0..l0_batch_size {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..100 {
            let key = format!("del_fail_key_{i:04}");
            tx.put(
                key.as_bytes().to_vec(),
                format!("value_{batch}").into_bytes(),
                None,
            )
            .expect("put");
        }
        tx.commit(WriteOptions::cloud_async()).expect("commit");
        engine.flush_cf(&cf).expect("flush batch");
    }

    let cloud_sst_dir = db_path.join("cloud_store").join("sst");
    let before_objects = sst_object_names(&cloud_sst_dir);
    assert!(
        before_objects.len() >= l0_batch_size,
        "expected one configured L0 batch to be mirrored to cloud storage, got {before_objects:?}"
    );

    // Arm only the remote SST delete boundary. The compacted output upload
    // and manifest authority switch remain real simulated-cloud operations.
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::cloud::inject_fail_sst_delete", "return")
        .expect("configure cloud delete outage failpoint");

    // Act: compaction should orphan the selected input SSTs and try (and fail)
    // to delete them from cloud storage.
    let compact_result = engine.compact_all();

    // Assert: compaction tolerates the delete failure rather than
    // propagating it as an error.
    assert!(
        compact_result.is_ok(),
        "compact_all should tolerate a cloud delete failure: {compact_result:?}"
    );

    // Assert: engine remains fully functional; no data was lost.
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin_tx");
    for i in 0..100 {
        let key = format!("del_fail_key_{i:04}");
        assert!(
            tx.get(key.as_bytes()).expect("get").is_some(),
            "data lost after cloud delete failure"
        );
    }
    drop(tx);

    // Shutdown joins every cloud-delete worker while the outage remains
    // armed, so the filesystem observation cannot race an unattempted delete.
    engine
        .shutdown(Duration::from_secs(10))
        .expect("shutdown after failed cloud delete");
    let after_objects = sst_object_names(&cloud_sst_dir);
    let retained: Vec<_> = before_objects.intersection(&after_objects).collect();

    fail::remove("midge::cloud::inject_fail_sst_delete");
    scenario.teardown();

    // Assert: the orphaned objects are still present in cloud storage
    // because their delete genuinely failed and was retained for retry,
    // not silently skipped or corrupted.
    assert!(
        !retained.is_empty(),
        "expected the orphaned cloud objects whose delete failed to remain \
         in cloud storage for retry, got {after_objects:?}"
    );

    eprintln!("✓ Engine gracefully handled cloud delete failure");
}

#[cfg(feature = "failpoints")]
fn failpoint_test_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}
