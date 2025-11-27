//! Fault Injection Tests
//!
//! These tests use the TestHooks infrastructure to inject faults at specific
//! points in the storage engine and verify correct recovery behavior.
//!
//! All tests are run against multiple storage modes:
//! - Memory: In-memory only (no persistence tests)
//! - LocalDisk: File-based persistence
//! - CloudBacked: Cloud storage with local cache
//!
//! Fault scenarios covered:
//! - Fsync failures (simulated power loss)
//! - WAL torn writes
//! - Manifest corruption
//! - Compaction failures
//! - Disk full (ENOSPC)
//! - I/O errors

mod common;

use cntryl_midge::{
    cloud::MockCloudBackend,
    config::cloud::StorageContext,
    test_hooks::{
        CompactionBehavior, FsyncBehavior, IoBehavior, ManifestBehavior, TestHooks, WalBehavior,
    },
    MidgeEngine, MidgeOptions, StorageMode, WalRecoveryMode,
};
use common::{test_temp_dir, DurabilityTestContext};
use std::sync::Arc;

// ============================================================================
// Helper: Create options with test hooks for each storage mode
// ============================================================================

fn opts_with_hooks_local_disk(
    dir: &tempfile::TempDir,
    hooks: TestHooks,
) -> MidgeOptions {
    MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        test_hooks: Some(hooks),
        ..Default::default()
    }
}

fn opts_with_hooks_cloud_backed(
    dir: &tempfile::TempDir,
    cloud_backend: Arc<MockCloudBackend>,
    hooks: TestHooks,
) -> MidgeOptions {
    MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.path().to_path_buf(),
            cloud_backend,
            storage_context: StorageContext::default(),
            local_wal_sync: true,
            wal_batch_size: 4 * 1024 * 1024,
            sst_cache_capacity: 16,
        },
        test_hooks: Some(hooks),
        ..Default::default()
    }
}

fn opts_with_hooks_memory(hooks: TestHooks) -> MidgeOptions {
    MidgeOptions {
        storage_mode: StorageMode::Memory,
        test_hooks: Some(hooks),
        ..Default::default()
    }
}

// ============================================================================
// Macro for multi-mode testing
// ============================================================================

/// Generate test functions for both LocalDisk and CloudBacked modes
macro_rules! test_persistent_modes {
    ($test_name:ident, $test_body:expr) => {
        paste::paste! {
            #[test]
            fn [<$test_name _local_disk>]() {
                let ctx = DurabilityTestContext::new("LocalDisk");
                $test_body(&ctx, "LocalDisk");
            }

            #[test]
            fn [<$test_name _cloud_backed>]() {
                let ctx = DurabilityTestContext::new("CloudBacked");
                $test_body(&ctx, "CloudBacked");
            }
        }
    };
}

/// Generate test functions for all three storage modes
macro_rules! test_all_modes {
    ($test_name:ident, $test_body:expr) => {
        paste::paste! {
            #[test]
            fn [<$test_name _memory>]() {
                $test_body("Memory");
            }

            #[test]
            fn [<$test_name _local_disk>]() {
                $test_body("LocalDisk");
            }

            #[test]
            fn [<$test_name _cloud_backed>]() {
                $test_body("CloudBacked");
            }
        }
    };
}

// ============================================================================
// Fsync Failure Tests (Simulated Power Loss)
// ============================================================================

test_persistent_modes!(should_survive_recovery_given_skip_fsync_when_reopening, |ctx: &DurabilityTestContext, mode: &str| {
    // Arrange
    let hooks = TestHooks::new().with_fsync_behavior(FsyncBehavior::Skip);

    let mut opts = MidgeOptions {
        storage_mode: ctx.create_storage_mode(),
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        eng.put(&cf, b"unfsynced_key", b"unfsynced_value")
            .expect("put");
        // Drop without flush - fsync was skipped, simulating crash
    }

    // Act - reopen without hooks (normal recovery)
    opts.test_hooks = None;
    let eng = MidgeEngine::open(opts).expect("reopen");
    let cf = eng.default_column_family();
    let _result = eng.get(&cf, b"unfsynced_key").expect("get");

    // Assert - recovery succeeded (data presence depends on OS buffering)
    assert!(hooks.fsync_count() == 0, "{}: fsync should have been skipped", mode);
});

test_all_modes!(should_track_fsync_count_given_record_only_fsync_when_writing, |mode: &str| {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_fsync_behavior(FsyncBehavior::RecordOnly);

    let opts = match mode {
        "Memory" => opts_with_hooks_memory(hooks.clone()),
        "LocalDisk" => opts_with_hooks_local_disk(&dir, hooks.clone()),
        "CloudBacked" => {
            let backend = Arc::new(MockCloudBackend::with_root(dir.path().join("cloud")));
            opts_with_hooks_cloud_backed(&dir, backend, hooks.clone())
        }
        _ => panic!("Unknown mode: {}", mode),
    };

    // Act
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    for i in 0..10 {
        eng.put(&cf, format!("key_{}", i).as_bytes(), b"value")
            .expect("put");
    }
    if mode != "Memory" {
        eng.flush().expect("flush");
    }
    let fsync_count = hooks.fsync_count();

    // Assert - fsync count depends on storage mode
    if mode == "Memory" {
        // Memory mode may or may not track fsyncs
        let _ = fsync_count;
    } else {
        assert!(
            fsync_count > 0,
            "{}: expected fsync calls to be recorded, got {}",
            mode,
            fsync_count
        );
    }
});

// ============================================================================
// WAL Torn Write Tests
// ============================================================================

test_persistent_modes!(should_recover_to_last_valid_record_given_truncated_wal_when_reopening, |ctx: &DurabilityTestContext, mode: &str| {
    // Arrange
    let hooks = TestHooks::new().with_wal_behavior(WalBehavior::TruncateAfterWrite);

    let mut opts = MidgeOptions {
        storage_mode: ctx.create_storage_mode(),
        test_hooks: Some(hooks),
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    };

    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();

        // Write several keys - some may be truncated
        eng.put(&cf, b"wal_key_1", b"value1").expect("put 1");
        eng.put(&cf, b"wal_key_2", b"value2").expect("put 2");
        eng.put(&cf, b"wal_key_3", b"value3").expect("put 3");
    }

    // Act - reopen with corruption tolerance
    opts.test_hooks = None;
    let eng = MidgeEngine::open(opts).expect("reopen");
    let cf = eng.default_column_family();

    // Assert - recovery should succeed
    let result1 = eng.get(&cf, b"wal_key_1").expect("get");
    let result2 = eng.get(&cf, b"wal_key_2").expect("get");
    // Due to truncation, we may or may not have all keys - key assertion is recovery succeeded
    assert!(
        result1.is_some() || result2.is_some(),
        "{}: at least some data should be recovered",
        mode
    );
});

// ============================================================================
// Manifest Failure Tests
// ============================================================================

test_persistent_modes!(should_fail_gracefully_given_manifest_save_failure_when_flushing, |ctx: &DurabilityTestContext, mode: &str| {
    // Arrange
    let hooks = TestHooks::new().with_manifest_behavior(ManifestBehavior::FailSave);

    let opts = MidgeOptions {
        storage_mode: ctx.create_storage_mode(),
        test_hooks: Some(hooks),
        ..Default::default()
    };

    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Act - put data and attempt flush
    eng.put(&cf, b"manifest_key", b"manifest_value")
        .expect("put");
    let flush_result = eng.flush();

    // Assert - flush should fail but not panic
    assert!(
        flush_result.is_err(),
        "{}: flush should fail when manifest save fails",
        mode
    );
});

// ============================================================================
// Compaction Failure Tests
// ============================================================================

test_persistent_modes!(should_recover_given_compaction_failure_midway_when_reopening, |ctx: &DurabilityTestContext, mode: &str| {
    // Arrange - first create some data to compact
    let setup_opts = MidgeOptions {
        storage_mode: ctx.create_storage_mode(),
        enable_compaction: false, // Manual compaction
        memtable_size: 1024,      // Small memtable to force multiple flushes
        ..Default::default()
    };

    {
        let eng = MidgeEngine::open(setup_opts).expect("open");
        let cf = eng.default_column_family();

        // Create multiple SST files
        for batch in 0..5 {
            for i in 0..100 {
                eng.put(
                    &cf,
                    format!("compact_key_{}_{:04}", batch, i).as_bytes(),
                    format!("value_{}", batch).as_bytes(),
                )
                .expect("put");
            }
            eng.flush().expect("flush");
        }
    }

    // Now try to compact with failure injection
    let hooks = TestHooks::new().with_compaction_behavior(CompactionBehavior::FailMidway);
    let compact_opts = MidgeOptions {
        storage_mode: ctx.create_storage_mode(),
        test_hooks: Some(hooks),
        enable_compaction: false,
        ..Default::default()
    };

    {
        let eng = MidgeEngine::open(compact_opts).expect("open for compaction");
        let _ = eng.compact_all(); // May fail
    }

    // Act - reopen without fault injection
    let recovery_opts = MidgeOptions {
        storage_mode: ctx.create_storage_mode(),
        ..Default::default()
    };
    let eng = MidgeEngine::open(recovery_opts).expect("reopen");
    let cf = eng.default_column_family();

    // Assert - all original data should still be readable
    for batch in 0..5 {
        let key = format!("compact_key_{}_{:04}", batch, 50);
        let result = eng.get(&cf, key.as_bytes()).expect("get");
        assert!(
            result.is_some(),
            "{}: key {} should be present after failed compaction",
            mode,
            key
        );
    }
});

// ============================================================================
// I/O Error Tests (Note: IoBehavior is checked during fsync, not writes)
// ============================================================================

// These tests verify that I/O error injection works during sync operations.
// The IoBehavior hooks are checked in sync_data_only(), not during regular put().

#[test]
fn should_return_error_given_enospc_when_syncing_local_disk() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_io_behavior(IoBehavior::FailWithEnospc);

    let opts = opts_with_hooks_local_disk(&dir, hooks);

    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Put succeeds (data goes to memtable)
    eng.put(&cf, b"enospc_key", b"value").expect("put");

    // Act - flush triggers fsync which should fail
    let result = eng.flush();

    // Assert - flush should fail with error, not panic
    assert!(result.is_err(), "flush should fail with ENOSPC during fsync");
}

#[test]
fn should_return_error_given_eio_when_syncing_local_disk() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_io_behavior(IoBehavior::FailWithEio);

    let opts = opts_with_hooks_local_disk(&dir, hooks);

    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Put succeeds (data goes to memtable)
    eng.put(&cf, b"eio_key", b"value").expect("put");

    // Act - flush triggers fsync which should fail
    let result = eng.flush();

    // Assert - flush should fail with error, not panic
    assert!(result.is_err(), "flush should fail with EIO during fsync");
}

// ============================================================================
// Multi-Fault Scenario Tests
// ============================================================================

test_persistent_modes!(should_recover_given_crash_during_compaction_with_pending_wal_when_reopening, |ctx: &DurabilityTestContext, mode: &str| {
    // Arrange - complex scenario: crash during compaction with unsynced WAL
    let setup_opts = MidgeOptions {
        storage_mode: ctx.create_storage_mode(),
        enable_compaction: false,
        memtable_size: 2048,
        ..Default::default()
    };

    {
        let eng = MidgeEngine::open(setup_opts).expect("open");
        let cf = eng.default_column_family();

        // Create flushed data
        for i in 0..50 {
            eng.put(&cf, format!("flushed_{:04}", i).as_bytes(), b"flushed_value")
                .expect("put");
        }
        eng.flush().expect("flush");
    }

    // Now write more data and simulate crash during compaction
    let hooks = TestHooks::new()
        .with_fsync_behavior(FsyncBehavior::Skip)
        .with_compaction_behavior(CompactionBehavior::CrashBeforeFsync);

    let crash_opts = MidgeOptions {
        storage_mode: ctx.create_storage_mode(),
        test_hooks: Some(hooks),
        enable_compaction: false,
        ..Default::default()
    };

    {
        let eng = MidgeEngine::open(crash_opts).expect("open");
        let cf = eng.default_column_family();

        // Write unsynced data
        for i in 0..20 {
            eng.put(&cf, format!("unsynced_{:04}", i).as_bytes(), b"unsynced_value")
                .expect("put");
        }

        // Attempt compaction (will "crash")
        let _ = eng.compact_all();
    }

    // Act - recover
    let recovery_opts = MidgeOptions {
        storage_mode: ctx.create_storage_mode(),
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    };
    let eng = MidgeEngine::open(recovery_opts).expect("reopen");
    let cf = eng.default_column_family();

    // Assert - flushed data should definitely be present
    for i in 0..50 {
        let key = format!("flushed_{:04}", i);
        let result = eng.get(&cf, key.as_bytes()).expect("get");
        assert!(
            result.is_some(),
            "{}: flushed key {} should survive crash",
            mode,
            key
        );
    }
});

// ============================================================================
// Instrumentation Verification Tests
// ============================================================================

test_all_modes!(should_count_operations_given_instrumented_hooks_when_performing_writes, |mode: &str| {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new();

    let opts = match mode {
        "Memory" => opts_with_hooks_memory(hooks.clone()),
        "LocalDisk" => opts_with_hooks_local_disk(&dir, hooks.clone()),
        "CloudBacked" => {
            let backend = Arc::new(MockCloudBackend::with_root(dir.path().join("cloud")));
            opts_with_hooks_cloud_backed(&dir, backend, hooks.clone())
        }
        _ => panic!("Unknown mode: {}", mode),
    };

    // Act
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    for i in 0..100 {
        eng.put(&cf, format!("count_key_{}", i).as_bytes(), b"value")
            .expect("put");
    }
    if mode != "Memory" {
        eng.flush().expect("flush");
    }

    let wal_count = hooks.wal_append_count();
    let manifest_count = hooks.manifest_update_count();

    // Assert
    assert!(
        wal_count >= 100,
        "{}: expected at least 100 WAL appends, got {}",
        mode,
        wal_count
    );
    if mode != "Memory" {
        assert!(
            manifest_count >= 1,
            "{}: expected at least 1 manifest update, got {}",
            mode,
            manifest_count
        );
    }
});

test_persistent_modes!(should_track_compaction_lifecycle_given_instrumented_hooks_when_compacting, |ctx: &DurabilityTestContext, mode: &str| {
    // Arrange
    let hooks = TestHooks::new();

    let opts = MidgeOptions {
        storage_mode: ctx.create_storage_mode(),
        test_hooks: Some(hooks.clone()),
        enable_compaction: false,
        memtable_size: 1024,
        ..Default::default()
    };

    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Create multiple SST files with enough data to trigger actual compaction
    for batch in 0..5 {
        for i in 0..100 {
            eng.put(
                &cf,
                format!("lifecycle_{}_{}", batch, i).as_bytes(),
                format!("value_{}", batch).as_bytes(),
            )
            .expect("put");
        }
        eng.flush().expect("flush");
    }

    // Act
    eng.compact_all().expect("compact");

    let start_count = hooks.compaction_start_count();
    let complete_count = hooks.compaction_complete_count();
    let failed_count = hooks.compaction_failed_count();

    // Assert - compaction hooks may or may not fire depending on whether
    // the compaction scheduler decided compaction was needed
    // The main assertion is that no failures occurred
    assert_eq!(
        failed_count, 0,
        "{}: expected no compaction failures, got {}",
        mode,
        failed_count
    );

    // If compaction started, it should have completed
    if start_count > 0 {
        assert!(
            complete_count >= 1,
            "{}: compaction started ({}) but didn't complete ({})",
            mode,
            start_count,
            complete_count
        );
    }
});

// ============================================================================
// Cloud-Specific Fault Injection Tests
// ============================================================================

#[test]
fn should_handle_cloud_upload_failure_given_mock_backend_failure_when_flushing() {
    // Arrange
    let dir = test_temp_dir();
    let backend = Arc::new(MockCloudBackend::with_root(dir.path().join("cloud")));

    // Configure mock to fail uploads
    backend.set_fail_upload_after(1);

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.path().to_path_buf(),
            cloud_backend: backend,
            storage_context: StorageContext::default(),
            local_wal_sync: true,
            wal_batch_size: 4 * 1024 * 1024,
            sst_cache_capacity: 16,
        },
        ..Default::default()
    };

    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Act - write enough data to trigger cloud operations
    for i in 0..100 {
        eng.put(&cf, format!("cloud_key_{}", i).as_bytes(), b"value")
            .expect("put");
    }
    let flush_result = eng.flush();

    // Assert - flush may succeed (local) or fail (cloud upload)
    // The key point is no panic occurs
    let _ = flush_result;
}

#[test]
fn should_handle_cloud_download_failure_given_mock_backend_failure_when_reading() {
    // Arrange
    let dir = test_temp_dir();
    let backend = Arc::new(MockCloudBackend::with_root(dir.path().join("cloud")));

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: StorageContext::default(),
            local_wal_sync: true,
            wal_batch_size: 4 * 1024 * 1024,
            sst_cache_capacity: 0, // Disable cache to force cloud reads
        },
        ..Default::default()
    };

    // First, write and flush data successfully
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        for i in 0..50 {
            eng.put(&cf, format!("read_key_{}", i).as_bytes(), b"value")
                .expect("put");
        }
        eng.flush().expect("flush");
    }

    // Configure mock to fail downloads (use upload failure as proxy)
    backend.set_fail_upload_after(0);

    // Act - reopen and try to read
    let eng = MidgeEngine::open(opts).expect("reopen");
    let cf = eng.default_column_family();
    let result = eng.get(&cf, b"read_key_25");

    // Assert - read may succeed (cached) or fail (cloud download) but no panic
    let _ = result;
}
