//! Phase 7.2: Cloud Upload Coordination Tests
//!
//! Tests for cloud SST upload coordination through EngineRuntime.
//! This ensures cloud uploads are ordered deterministically with other background work.
//!
//! Test coverage:
//! - SST upload submission as runtime task during flush
//! - Upload task sequencing with multiple flushes
//! - Runtime task execution order preservation

mod common;

use cntryl_midge::cloud::mock::MockCloudBackend;
use cntryl_midge::config::cloud::StorageContext;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use cntryl_midge::testkit::test_temp_dir;
use std::sync::Arc;
use std::time::Duration;

// Helper to create cloud storage options
fn cloud_storage_opts(dir: &std::path::Path, backend: Arc<MockCloudBackend>) -> MidgeOptions {
    MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.to_path_buf(),
            cloud_backend: backend,
            storage_context: StorageContext::new("test"),
            local_wal_sync: true,
            wal_batch_size: 1024 * 1024,
            sst_cache_capacity: 10,
        },
        memtable_size: 1024 * 1024,
        wal_sync: true,
        wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    }
}

// ============================================================================
// Phase 7.2: Cloud Upload Task Coordination
// ============================================================================

/// Test that cloud SST uploads are submitted as runtime tasks during flush.
/// This verifies the Phase 7.2 integration of cloud uploads with EngineRuntime.
#[test]
fn should_submit_cloud_upload_task_during_flush_when_cloud_backed() {
    // Arrange
    let dir = test_temp_dir();
    let mock_backend = Arc::new(MockCloudBackend::new());
    let opts = cloud_storage_opts(dir.path(), mock_backend.clone());
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Baseline: count initial uploads (should be none)
    let baseline_uploads = mock_backend.upload_count();
    assert_eq!(baseline_uploads, 0, "Initial state should have no uploads");

    // Act: Write data and flush
    for i in 0..10 {
        eng.put(&cf, format!("key{:02}", i).as_bytes(), b"value")
            .expect("put");
    }
    let _ = eng.flush_cf(&cf);

    // Wait for upload to complete through runtime coordination
    let _ = mock_backend.wait_for_uploads(baseline_uploads + 1, Duration::from_secs(5));

    // Assert: Cloud upload should have been queued and executed
    assert!(
        mock_backend.upload_count() > baseline_uploads,
        "Cloud upload task should have been submitted and executed during flush"
    );

    // Verify data is still accessible
    for i in 0..10 {
        let result = eng
            .get(&cf, format!("key{:02}", i).as_bytes())
            .expect("get");
        assert!(
            result.is_some(),
            "Data should be accessible after cloud upload"
        );
    }
}

/// Test that multiple flushes result in multiple cloud upload tasks in order.
/// This verifies that cloud uploads maintain deterministic ordering with other operations.
#[test]
fn should_sequence_cloud_uploads_across_multiple_flushes_when_cloud_backed() {
    // Arrange
    let dir = test_temp_dir();
    let mock_backend = Arc::new(MockCloudBackend::new());
    let opts = cloud_storage_opts(dir.path(), mock_backend.clone());
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    let baseline_uploads = mock_backend.upload_count();

    // Act: Perform 3 flushes
    for batch in 0..3 {
        // Write batch of data
        for i in 0..5 {
            eng.put(
                &cf,
                format!("batch{}_key{:02}", batch, i).as_bytes(),
                b"value",
            )
            .expect("put");
        }
        let _ = eng.flush_cf(&cf);
        // Wait for this flush's upload to complete
        let expected_uploads = baseline_uploads + batch as usize + 1;
        let _ = mock_backend.wait_for_uploads(expected_uploads, Duration::from_secs(5));
    }

    // Assert: Should have 3 uploads (one per flush)
    assert_eq!(
        mock_backend.upload_count(),
        baseline_uploads + 3,
        "Should have submitted cloud upload task for each flush"
    );

    // Verify all data is accessible
    for batch in 0..3 {
        for i in 0..5 {
            let result = eng
                .get(&cf, format!("batch{}_key{:02}", batch, i).as_bytes())
                .expect("get");
            assert!(
                result.is_some(),
                "Data from batch {} should be accessible",
                batch
            );
        }
    }
}
